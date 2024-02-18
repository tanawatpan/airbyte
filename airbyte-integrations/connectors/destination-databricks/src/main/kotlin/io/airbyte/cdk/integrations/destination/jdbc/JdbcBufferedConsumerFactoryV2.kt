package io.airbyte.cdk.integrations.destination.jdbc

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.db.factory.DataSourceFactory
import io.airbyte.cdk.db.jdbc.JdbcDatabase
import io.airbyte.cdk.db.jdbc.JdbcUtils
import io.airbyte.cdk.integrations.base.SerializedAirbyteMessageConsumer
import io.airbyte.cdk.integrations.destination.NamingConventionTransformer
import io.airbyte.cdk.integrations.destination.async.buffers.BufferManager
import io.airbyte.cdk.integrations.destination.async.deser.DeserializationUtil
import io.airbyte.cdk.integrations.destination.async.deser.IdentityDataTransformer
import io.airbyte.cdk.integrations.destination.async.partial_messages.PartialAirbyteMessage
import io.airbyte.cdk.integrations.destination.async.state.FlushFailure
import io.airbyte.cdk.integrations.destination.buffered_stream_consumer.OnCloseFunction
import io.airbyte.cdk.integrations.destination.buffered_stream_consumer.OnStartFunction
import io.airbyte.cdk.integrations.destination_async.AsyncStreamConsumerV2
import io.airbyte.commons.json.Jsons
import io.airbyte.integrations.destination.databricks.DatabricksDestinationConfig
import io.airbyte.integrations.destination.databricks.DatabricksService
import io.airbyte.integrations.destination.databricks.DatabricksSqlOperations
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import java.util.*
import java.util.function.Consumer
import javax.sql.DataSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory

@OptIn(ExperimentalCoroutinesApi::class)
object JdbcBufferedConsumerFactoryV2 {

    private val logger = LoggerFactory.getLogger(this::class.java)
    private val parallelism = Runtime.getRuntime().availableProcessors()
    private val dispatcher = Dispatchers.IO.limitedParallelism(parallelism)

    @JvmStatic
    fun createAsync(
        outputRecordCollector: Consumer<AirbyteMessage?>,
        dataSource: DataSource,
        database: JdbcDatabase,
        service: DatabricksService,
        sqlOperations: DatabricksSqlOperations,
        namingResolver: NamingConventionTransformer,
        config: JsonNode,
        databricksConfig: DatabricksDestinationConfig,
        catalog: ConfiguredAirbyteCatalog,
        defaultNamespace: Optional<String>
    ): SerializedAirbyteMessageConsumer {
        logger.info("Parallelism: $parallelism")
        val writeConfigs =
            createWriteConfigs(namingResolver, config, catalog, sqlOperations.isSchemaRequired)
        return AsyncStreamConsumerV2(
            outputRecordCollector,
            onStartFunction(database, sqlOperations, writeConfigs, databricksConfig),
            onCloseFunction(
                dataSource,
                database,
                service,
                sqlOperations,
                databricksConfig,
                writeConfigs,
                namingResolver,
            ),
            JdbcInsertFlushFunction(
                recordWriterFunction(
                    database,
                    sqlOperations,
                    writeConfigs,
                    catalog,
                ),
            ),
            FlushFailure(),
            catalog,
            BufferManager((Runtime.getRuntime().maxMemory() * 0.3).toLong()),
            defaultNamespace,
            IdentityDataTransformer(),
            DeserializationUtil(),
            dispatcher,
        )
    }

    private fun createWriteConfigs(
        namingResolver: NamingConventionTransformer,
        config: JsonNode,
        catalog: ConfiguredAirbyteCatalog,
        schemaRequired: Boolean
    ): List<WriteConfig> {
        if (schemaRequired) {
            check(config.has("schema")) { "jdbc destinations must specify a schema." }
        }
        return catalog.streams.map { stream ->
            val defaultSchemaName = when (schemaRequired) {
                true -> namingResolver.getIdentifier(config.get("schema").asText())
                false -> namingResolver.getIdentifier(config.get(JdbcUtils.DATABASE_KEY).asText())
            }
            val syncMode = stream.destinationSyncMode
            val streamName = stream.stream.name
            val tableName = namingResolver.getRawTableName(streamName)
            val tmpTableName = namingResolver.getTmpTableName(streamName)
            val outputSchema = getOutputSchema(stream.stream, defaultSchemaName, namingResolver)

            checkNotNull(syncMode) { "Undefined destination sync mode" }

            WriteConfig(
                streamName,
                stream.stream.namespace,
                outputSchema,
                tmpTableName,
                tableName,
                syncMode,
            ).also { logger.info("Write config: $it") }
        }
    }

    private fun getOutputSchema(
        stream: AirbyteStream,
        defaultDestSchema: String,
        namingResolver: NamingConventionTransformer
    ): String = namingResolver.getNamespace(stream.namespace ?: defaultDestSchema)

    private fun onStartFunction(
        database: JdbcDatabase,
        sqlOperations: DatabricksSqlOperations,
        writeConfigs: List<WriteConfig>,
        databricksConfig: DatabricksDestinationConfig
    ) = OnStartFunction {
        runBlocking {
            logger.info("Preparing raw tables in destination started for ${writeConfigs.size} streams")

            writeConfigs.map { it.outputSchemaName }.distinct().forEach { schemaName ->
                sqlOperations.createSchemaIfNotExists(
                    database,
                    databricksConfig.catalog,
                    schemaName,
                )
            }

            writeConfigs.map { writeConfig ->
                async(dispatcher) {
                    val schemaName = writeConfig.outputSchemaName
                    val dstTableName = writeConfig.tmpTableName
                    sqlOperations.dropTableIfExists(
                        database,
                        databricksConfig.catalog,
                        schemaName,
                        dstTableName,
                    )
                    sqlOperations.createTableIfNotExists(
                        database,
                        databricksConfig.catalog,
                        schemaName,
                        dstTableName,
                    )
                }
            }.awaitAll()

            logger.info("Preparing raw tables in destination completed.")
        }
    }

    private fun recordWriterFunction(
        database: JdbcDatabase,
        sqlOperations: SqlOperations,
        writeConfigs: List<WriteConfig>,
        catalog: ConfiguredAirbyteCatalog
    ): (AirbyteStreamNameNamespacePair, List<PartialAirbyteMessage>) -> Unit {
        val pairToWriteConfig = writeConfigs.associateBy { toNameNamespacePair(it) }
        return { pair, records ->
            val writeConfig = pairToWriteConfig[pair]
                ?: throw IllegalArgumentException(
                    "Message contained record from a stream that was not in the catalog. \ncatalog: ${
                        Jsons.serialize(
                            catalog,
                        )
                    }",
                )
            sqlOperations.insertRecords(
                database,
                records,
                writeConfig.outputSchemaName,
                writeConfig.tmpTableName,
            )
        }
    }

    private fun onCloseFunction(
        dataSource: DataSource,
        jdbc: JdbcDatabase,
        service: DatabricksService,
        sqlOperations: DatabricksSqlOperations,
        databricksConfig: DatabricksDestinationConfig,
        writeConfigs: List<WriteConfig>,
        namingResolver: NamingConventionTransformer
    ) =
        OnCloseFunction { hasFailed, _ ->
            runBlocking {
                try {
                    if (!hasFailed) {
                        writeConfigs.map { config ->
                            async(dispatcher) {
                                val tableName = namingResolver.getIdentifier(config.streamName)

                                val tmpTableSchemaString = sqlOperations.getTableSchemaString(
                                    jdbc,
                                    databricksConfig.catalog,
                                    config.namespace,
                                    config.tmpTableName,
                                )
                                val tmpTableSchema = sqlOperations.parseSchema(tmpTableSchemaString)

                                val unpackJsonQuery = sqlOperations.getUnpackJsonQuery(
                                    databricksConfig.catalog,
                                    config.namespace,
                                    config.tmpTableName,
                                    tmpTableSchemaString,
                                    false,
                                )

                                jdbc.execute(
                                    "CREATE TABLE IF NOT EXISTS ${databricksConfig.catalog}.${config.namespace}.${tableName} AS $unpackJsonQuery LIMIT 0 ;",
                                )

                                val targetTableSchema = sqlOperations.describeTableSchema(
                                    jdbc,
                                    databricksConfig.catalog,
                                    config.namespace,
                                    tableName,
                                )

                                listOf(
                                    sqlOperations.finalizeTableQuery(
                                        config.syncMode,
                                        tmpTableSchema,
                                        targetTableSchema,
                                        databricksConfig.catalog,
                                        config.namespace,
                                        config.tmpTableName,
                                        tableName,
                                    ),
                                )
                            }
                        }.awaitAll().map { queries ->
                            async(dispatcher) {
                                sqlOperations.executeTransaction(jdbc, queries)
                            }
                        }.awaitAll()

                        writeConfigs.map { config ->
                            async(dispatcher) {
                                val tableName = namingResolver.getIdentifier(config.streamName)
                                sqlOperations.optimizeTable(
                                    databricksConfig,
                                    config.namespace,
                                    tableName,
                                    jdbc,
                                )
                                sqlOperations.vacuumTable(
                                    databricksConfig,
                                    config.namespace,
                                    tableName,
                                    jdbc,
                                )
                            }
                        }.awaitAll()
                    }
                } catch (e: Exception) {
                    throw RuntimeException(e)
                } finally {
                    if (databricksConfig.isPurgeStagingData) {
                        writeConfigs.map { each ->
                            async(dispatcher) {
                                sqlOperations.dropTableIfExists(
                                    jdbc,
                                    databricksConfig.catalog,
                                    each.namespace,
                                    each.tmpTableName,
                                )
                            }
                        }.awaitAll()
                    }
                    DataSourceFactory.close(dataSource)
                    service.stopClusterIfIdle()
                }
            }
        }

    private fun toNameNamespacePair(config: WriteConfig) =
        AirbyteStreamNameNamespacePair(config.streamName, config.namespace)
}
