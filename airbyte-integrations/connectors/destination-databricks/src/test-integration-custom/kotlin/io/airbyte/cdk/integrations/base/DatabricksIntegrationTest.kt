package io.airbyte.cdk.integrations.base

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import io.airbyte.cdk.db.Database
import io.airbyte.cdk.db.factory.DataSourceFactory
import io.airbyte.cdk.db.jdbc.JdbcUtils
import io.airbyte.cdk.integrations.destination.StandardNameTransformer
import io.airbyte.cdk.integrations.destination.jdbc.copy.StreamCopierFactory
import io.airbyte.cdk.integrations.destination.s3.util.AvroRecordHelper
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.resources.MoreResources
import io.airbyte.integrations.destination.databricks.DatabricksDestination
import io.airbyte.integrations.destination.databricks.DatabricksDestinationConfig
import io.airbyte.integrations.destination.databricks.DatabricksDestinationResolver
import io.airbyte.integrations.destination.databricks.DatabricksNameTransformer
import io.airbyte.integrations.destination.databricks.utils.DatabricksDatabaseUtil
import io.airbyte.protocol.models.v0.*
import io.mockk.every
import io.mockk.mockkStatic
import io.mockk.slot
import io.mockk.spyk
import java.sql.SQLException
import java.util.function.Consumer
import javax.sql.DataSource
import kotlin.test.assertEquals
import org.jooq.DSLContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.Logger
import org.slf4j.LoggerFactory


@ExtendWith(DatabricksIntegrationTestExtension::class)
abstract class DatabricksIntegrationTest {
    protected val logger: Logger = LoggerFactory.getLogger(this::class.java)

    abstract val configFile: String

    protected val nameTransformer: StandardNameTransformer = DatabricksNameTransformer()
    protected lateinit var databricksConfigJson: JsonNode
    protected lateinit var databricksConfig: DatabricksDestinationConfig
    protected lateinit var destination: DatabricksDestination
    protected lateinit var dataSource: DataSource
    protected lateinit var dslContext: DSLContext
    protected lateinit var database: Database
    private val mapper = ObjectMapper().apply {
        configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
    }

    @BeforeEach
    open fun setUp() {
        databricksConfigJson = Jsons.deserialize(MoreResources.readResource(configFile))
        databricksConfig = DatabricksDestinationConfig[databricksConfigJson]
        dataSource = DatabricksDatabaseUtil.getDataSource(databricksConfig)
        dslContext = DatabricksDatabaseUtil.getDslContext(dataSource)
        database = Database(dslContext)
    }

    fun testSync(catalogFile: String, messageFile: String) {
        val configuredCatalog = getConfiguredCatalog(catalogFile)
        val messages = MoreResources.readResource(messageFile).lines().map {
            Jsons.deserialize(it, AirbyteMessage::class.java)
        }

        val outputRecordCollector = slot<Consumer<AirbyteMessage>>()
        destination = spyk(DatabricksDestination()) {
            every {
                getSerializedMessageConsumer(any(), any(), capture(outputRecordCollector))
            } answers {
                val type = DatabricksDestinationResolver.getTypeFromConfig(databricksConfigJson)
                val dst = DatabricksDestinationResolver.getTypeToDestination()[type]
                dst?.getSerializedMessageConsumer(
                    databricksConfigJson,
                    configuredCatalog,
                    outputRecordCollector.captured,
                )
            }
        }
        logger.info("Destination: ${destination.javaClass.simpleName}")

        System.setIn(MoreResources.readBytes(messageFile).inputStream())

        mockkStatic("io.airbyte.cdk.integrations.base.IntegrationRunner")
        every { IntegrationRunner.stopOrphanedThreads() } returns Unit

        IntegrationRunner(destination).run(
            arrayOf(
                "--write",
                "--config",
                "src/test-integration-custom/resources/$configFile",
                "--catalog",
                "src/test-integration-custom/resources/$catalogFile",
            ),
        )

        configuredCatalog.streams.forEach { configuredStream ->
            val schema = configuredStream.stream.jsonSchema
            val namespace = databricksConfig.schema
            val streamName = configuredStream.stream.name

            val actualMessages = retrieveRecords(
                streamName,
                namespace,
                schema,
            ).map {
                AirbyteRecordMessage().withStream(streamName).withData(it)
            }
            logger.info("Actual messages size: ${actualMessages.size}")

            val expected = messages.filter { it.record?.stream == streamName }
            assertSameMessages(expected, actualMessages)
        }
    }

    @AfterEach
    open fun tearDown() {
        cleanUpData(databricksConfig)
    }

    open fun cleanUpData(databricksConfig: DatabricksDestinationConfig) {
        try {
            val database = Database(dslContext)
            database.query { ctx ->
                ctx.execute("DROP SCHEMA IF EXISTS ${databricksConfig.catalog}.${databricksConfig.schema} CASCADE;")
            }
        } catch (e: Exception) {
            throw SQLException(e)
        } finally {
            DataSourceFactory.close(dataSource)
        }
    }

    @Throws(SQLException::class)
    open fun retrieveRecords(
        streamName: String,
        namespace: String,
        streamSchema: JsonNode,
    ): List<JsonNode> {
        val nameUpdater = AvroRecordHelper.getFieldNameUpdater(streamName, namespace, streamSchema)
        val tableName = nameTransformer.getIdentifier(streamName)
        val schemaName =
            StreamCopierFactory.getSchema(namespace, databricksConfig.schema, nameTransformer)
        val catalog = databricksConfig.catalog
        val columns = streamSchema.get("properties").fields().asSequence().map {
            Pair(it.key, it.value.get("type").asText())
        }
        val selectColumns = getSelectColumns(columns)

        return database.query { ctx ->
            ctx.fetch(
                """
                SELECT $selectColumns
                FROM $catalog.$schemaName.$tableName
                ORDER BY ${JavaBaseConstants.COLUMN_NAME_EMITTED_AT} ASC
            """.trimIndent(),
            ).map { record ->
                Jsons.deserialize(record.formatJSON(JdbcUtils.getDefaultJSONFormat())).let { json ->
                    nameUpdater.getJsonWithOriginalFieldNames(json).let { jsonWithOriginalFields ->
                        AvroRecordHelper.pruneAirbyteJson(jsonWithOriginalFields)
                    }
                }
            }
        }
    }

    open fun getSelectColumns(columns: Sequence<Pair<String, String>>) =
        columns.map { (column, type) ->
            val columnType = type.lowercase().trim()
            if (columnType.contains("date") || columnType.contains("time")) {
                "date_format(`$column`, \"yyyy-MM-dd'T'HH:mm:ss'Z'\") AS `$column`"
            } else {
                "`$column`"
            }
        }.joinToString(", ")

    private fun getConfiguredCatalog(catalogFile: String): ConfiguredAirbyteCatalog {
        val catalog =
            Jsons.deserialize(MoreResources.readResource(catalogFile), AirbyteCatalog::class.java)
        return CatalogHelpers.toDefaultConfiguredCatalog(catalog)
    }

    private fun assertSameMessages(
        expected: List<AirbyteMessage>,
        actual: List<AirbyteRecordMessage>,
    ) {
        val expectedJson = expected.filter { it.type == AirbyteMessage.Type.RECORD }
            .map { it.record.apply { emittedAt = null } }
            .map { mapper.writeValueAsString(sortJsonNode(it.data)) }.sorted()

        val actualJson = actual.map {
            mapper.writeValueAsString(sortJsonNode(it.data))
        }.sorted()

        assertEquals(
            expectedJson.size,
            actualJson.size,
            "Expected Size: ${expectedJson.size}\nActual Size: ${actualJson.size}",
        )

        assertEquals(
            expectedJson,
            actualJson,
            "Expected: $expectedJson\nActual: $actualJson",
        )
    }

    private fun sortJsonNode(node: JsonNode): JsonNode {
        if (node.isObject) {
            val sortedObject = mapper.createObjectNode()
            val fieldNames = node.fieldNames().asSequence().toList().sorted()
            fieldNames.forEach { fieldName ->
                sortedObject.set<JsonNode>(fieldName, sortJsonNode(node.get(fieldName)))
            }
            return sortedObject
        } else if (node.isArray) {
            val sortedArray = mapper.createArrayNode()
            node.forEach { element ->
                sortedArray.add(sortJsonNode(element))
            }
            return sortedArray
        }

        return node
    }
}
