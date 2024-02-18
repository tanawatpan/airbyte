package io.airbyte.cdk.integrations.destination.jdbc

import io.airbyte.cdk.db.factory.DataSourceFactory
import io.airbyte.cdk.db.jdbc.JdbcDatabase
import io.airbyte.cdk.integrations.base.AirbyteMessageConsumer
import io.airbyte.cdk.integrations.destination.StandardNameTransformer
import io.airbyte.cdk.integrations.destination.buffered_stream_consumer.*
import io.airbyte.cdk.integrations.destination.jdbc.constants.GlobalDataSizeConstants
import io.airbyte.cdk.integrations.destination.jdbc.copy.StreamCopierFactory
import io.airbyte.cdk.integrations.destination.record_buffer.InMemoryRecordBufferingStrategy
import io.airbyte.integrations.destination.databricks.DatabricksDestinationConfig
import io.airbyte.integrations.destination.databricks.DatabricksService
import io.airbyte.integrations.destination.databricks.DatabricksSqlOperations
import io.airbyte.integrations.destination.databricks.DatabricksStreamCopier
import io.airbyte.protocol.models.v0.*
import java.util.*
import java.util.function.Consumer
import javax.sql.DataSource
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory

@OptIn(ExperimentalCoroutinesApi::class)
object CopyConsumerFactoryV2 {

    private val logger = LoggerFactory.getLogger(this::class.java)
    private val parallelism = Runtime.getRuntime().availableProcessors()
    private val dispatcher = Dispatchers.IO.limitedParallelism(parallelism)

    @JvmStatic
    fun create(
        outputRecordCollector: Consumer<AirbyteMessage>,
        dataSource: DataSource,
        database: JdbcDatabase,
        service: DatabricksService,
        sqlOperations: DatabricksSqlOperations,
        namingResolver: StandardNameTransformer,
        config: DatabricksDestinationConfig,
        catalog: ConfiguredAirbyteCatalog,
        streamCopierFactory: StreamCopierFactory<DatabricksDestinationConfig>,
        defaultSchema: String
    ): AirbyteMessageConsumer {
        logger.info("Parallelism: $parallelism")
        val pairToCopier = createWriteConfigs(
            namingResolver,
            config,
            catalog,
            streamCopierFactory,
            defaultSchema,
            database,
            sqlOperations,
        )
        val pairToIgnoredRecordCount: MutableMap<AirbyteStreamNameNamespacePair, Long> = HashMap()
        return BufferedStreamConsumer(
            outputRecordCollector,
            onStartFunction(pairToIgnoredRecordCount),
            InMemoryRecordBufferingStrategy(
                recordWriterFunction(pairToCopier, sqlOperations, pairToIgnoredRecordCount),
                removeStagingFilePrinter(pairToCopier),
                GlobalDataSizeConstants.DEFAULT_MAX_BATCH_SIZE_BYTES.toLong(),
            ),
            onCloseFunction(
                pairToCopier,
                pairToIgnoredRecordCount,
                database,
                dataSource,
                service,
                sqlOperations,
                config,
            ),
            catalog,
            sqlOperations::isValidData,
            null,
        )
    }

    private fun <T> createWriteConfigs(
        namingResolver: StandardNameTransformer,
        config: T,
        catalog: ConfiguredAirbyteCatalog,
        streamCopierFactory: StreamCopierFactory<T>,
        defaultSchema: String,
        database: JdbcDatabase,
        sqlOperations: SqlOperations
    ): Map<AirbyteStreamNameNamespacePair, DatabricksStreamCopier> {
        val stagingFolder = UUID.randomUUID().toString()
        return HashMap<AirbyteStreamNameNamespacePair, DatabricksStreamCopier>().apply {
            catalog.streams.forEach { configuredStream ->
                val stream = configuredStream.stream
                val pair = AirbyteStreamNameNamespacePair.fromAirbyteStream(stream)
                val copier = streamCopierFactory.create(
                    defaultSchema,
                    config,
                    stagingFolder,
                    configuredStream,
                    namingResolver,
                    database,
                    sqlOperations,
                )
                put(pair, copier as DatabricksStreamCopier)
            }
        }
    }

    private fun onStartFunction(pairToIgnoredRecordCount: MutableMap<AirbyteStreamNameNamespacePair, Long>) =
        OnStartFunction { pairToIgnoredRecordCount.clear() }

    private fun recordWriterFunction(
        pairToCopier: Map<AirbyteStreamNameNamespacePair, DatabricksStreamCopier>,
        sqlOperations: SqlOperations,
        pairToIgnoredRecordCount: MutableMap<AirbyteStreamNameNamespacePair, Long>
    ) = RecordWriter { pair: AirbyteStreamNameNamespacePair, records: List<AirbyteRecordMessage> ->
        pairToIgnoredRecordCount.apply {
            val fileName = pairToCopier[pair]?.prepareStagingFile()
            for (recordMessage in records) {
                when {
                    sqlOperations.isValidData(recordMessage.data) -> pairToCopier[pair]?.write(
                        UUID.randomUUID(),
                        recordMessage,
                        fileName,
                    )

                    else -> put(pair, getOrDefault(pair, 0L) + 1L)
                }
            }
        }
    }

    private fun removeStagingFilePrinter(pairToCopier: Map<AirbyteStreamNameNamespacePair, DatabricksStreamCopier>) =
        CheckAndRemoveRecordWriter { pair: AirbyteStreamNameNamespacePair, stagingFileName: String? ->
            val currentFileName = pairToCopier[pair]?.currentFile
            if (stagingFileName != null && currentFileName != null && stagingFileName != currentFileName) {
                pairToCopier[pair]?.closeNonCurrentStagingFileWriters()
            }
            currentFileName
        }

    private fun onCloseFunction(
        pairToCopier: Map<AirbyteStreamNameNamespacePair, DatabricksStreamCopier>,
        pairToIgnoredRecordCount: Map<AirbyteStreamNameNamespacePair, Long>,
        database: JdbcDatabase,
        dataSource: DataSource,
        service: DatabricksService,
        sqlOperations: DatabricksSqlOperations,
        databricksConfig: DatabricksDestinationConfig
    ) = OnCloseFunction { hasFailed, _ ->
        pairToIgnoredRecordCount.forEach { (pair, count) ->
            logger.warn(
                "A total of {} record(s) of data from stream {} were invalid and were ignored.",
                count,
                pair,
            )
        }
        closeAsOneTransaction(
            pairToCopier,
            hasFailed,
            database,
            dataSource,
            service,
            sqlOperations,
            databricksConfig,
        )
    }

    @Throws(Exception::class)
    private fun closeAsOneTransaction(
        pairToCopier: Map<AirbyteStreamNameNamespacePair, DatabricksStreamCopier>,
        hasFailed: Boolean,
        jdbc: JdbcDatabase,
        dataSource: DataSource,
        service: DatabricksService,
        sqlOperations: DatabricksSqlOperations,
        databricksConfig: DatabricksDestinationConfig,
    ) = runBlocking {
        val streamCopiers = pairToCopier.values.toList()

        try {
            streamCopiers.map { it.schemaName }.distinct().forEach { schemaName ->
                sqlOperations.createSchemaIfNotExists(jdbc, databricksConfig.catalog, schemaName)
            }
            streamCopiers.map { copier ->
                async(dispatcher) {
                    copier.closeStagingUploader(hasFailed)
                    if (!hasFailed) {
                        copier.createTemporaryTable()
                        copier.copyStagingFileToTemporaryTable()
                        val destTableName = copier.createDestinationTable()
                        val mergeQuery = copier.generateMergeStatement(destTableName)
                        val schemaName = copier.schemaName
                        listOf(schemaName, destTableName, mergeQuery)
                    } else emptyList()
                }
            }.awaitAll().filter {
                it.isNotEmpty()
            }.map { (schemaName, tableName, mergeQuery) ->
                async(dispatcher) {
                    logger.info(mergeQuery)
                    jdbc.execute(mergeQuery)
                    sqlOperations.optimizeTable(
                        databricksConfig,
                        schemaName,
                        tableName,
                        jdbc,
                    )
                    sqlOperations.vacuumTable(
                        databricksConfig,
                        schemaName,
                        tableName,
                        jdbc,
                    )
                }
            }.awaitAll()
        } catch (e: Exception) {
            throw RuntimeException(e)
        } finally {
            streamCopiers.map {
                async(dispatcher) { it.removeFileAndDropTmpTable() }
            }.awaitAll()

            DataSourceFactory.close(dataSource)
            service.stopClusterIfIdle()
        }
    }
}
