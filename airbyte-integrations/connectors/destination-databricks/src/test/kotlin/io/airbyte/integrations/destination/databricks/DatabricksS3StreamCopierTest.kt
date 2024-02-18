package io.airbyte.integrations.destination.databricks

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.cdk.db.jdbc.DefaultJdbcDatabase
import io.airbyte.cdk.integrations.destination.StandardNameTransformer
import io.airbyte.cdk.integrations.destination.s3.S3DestinationConfig
import io.airbyte.cdk.integrations.destination.s3.avro.JsonToAvroSchemaConverter
import io.airbyte.cdk.integrations.destination.s3.parquet.S3ParquetWriter
import io.airbyte.cdk.integrations.destination.s3.writer.ProductionWriterFactory
import io.airbyte.integrations.destination.databricks.s3.DatabricksS3StreamCopierV2
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.junit5.MockKExtension
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import java.sql.Timestamp
import kotlin.test.Test
import kotlin.test.assertEquals
import org.apache.avro.Schema
import org.apache.commons.lang3.RandomStringUtils
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(MockKExtension::class)
class DatabricksS3StreamCopierTest {

    private lateinit var configuredStream: ConfiguredAirbyteStream
    private lateinit var jdbcDatabase: DefaultJdbcDatabase
    private lateinit var databricksDestinationConfig: DatabricksDestinationConfig
    private lateinit var nameTransformer: StandardNameTransformer
    private lateinit var sqlOperations: DatabricksSqlOperations
    private lateinit var writerFactory: ProductionWriterFactory
    private lateinit var databricksS3StreamCopier: DatabricksS3StreamCopierV2

    private val region = Regions.US_EAST_1
    private val bucketName = "seac-databricks-prd"
    private val bucketPath = "hive_metastore/test_db"
    private val catalog = "catalog"
    private val schemaName = "schema"
    private val streamName = "stream"
    private val namespace = "namespace"
    private val tmpTableName = "_airbyte_tmp_random_stream"
    private val mode = DestinationSyncMode.OVERWRITE

    private val expectedSelectColumns = "_airbyte_ab_id, _airbyte_emitted_at, " +
        "key, name, isActive, CAST(createdAt.member0 AS TIMESTAMP) AS createdAt, CAST(file_date.member0 AS TIMESTAMP) AS file_date, CAST(updatedAt.member0 AS TIMESTAMP) AS updatedAt, " +
        "_airbyte_additional_properties"

    private val avroSchema: Schema = JsonToAvroSchemaConverter().getAvroSchema(
        ObjectMapper().readTree(
            """
        {
          "type": "object",
          "properties": {
            "key": { "type": "string" },
            "name": { "type": "string" },
            "isActive": { "type": "boolean" },
            "createdAt": {
              "type": "string",
              "format": "date-time",
              "airbyte_type": "timestamp_without_timezone"
            },
            "file_date": { "type": "string", "format": "date" },
            "updatedAt": {
              "type": "string",
              "format": "date-time",
              "airbyte_type": "timestamp_without_timezone"
            }
          }
        }
        """.trimIndent(),
        ),
        streamName, namespace,
    )

    @BeforeEach
    fun setUp() {
        mockkStatic(RandomStringUtils::class)
        every { RandomStringUtils.randomAlphabetic(any()) } returns "random"

        configuredStream = mockk<ConfiguredAirbyteStream> {
            every { stream.name } returns streamName
            every { destinationSyncMode } returns mode
        }
        jdbcDatabase = mockk<DefaultJdbcDatabase>(relaxed = true)
        databricksDestinationConfig = mockk<DatabricksDestinationConfig>(relaxed = true) {
            every { storageConfig?.s3DestinationConfigOrThrow } returns S3DestinationConfig(
                null,
                bucketName,
                bucketPath,
                region.getName(),
                null,
                null,
                null,
                null,
            )
        }
        nameTransformer = DatabricksNameTransformer()
        sqlOperations = mockk<DatabricksSqlOperations>(relaxed = true)
        writerFactory = mockk<ProductionWriterFactory>(relaxed = true) {
            every {
                create(
                    any<S3DestinationConfig>(),
                    any<AmazonS3>(),
                    any<ConfiguredAirbyteStream>(),
                    any<Timestamp>(),
                )
            } returns mockk<S3ParquetWriter>(relaxed = true) {
                every { schema } returns avroSchema
            }
        }

        databricksS3StreamCopier = spyk(
            DatabricksS3StreamCopierV2(
                "",
                catalog,
                schemaName,
                configuredStream,
                mockk<AmazonS3>(relaxed = true),
                jdbcDatabase,
                databricksDestinationConfig,
                nameTransformer,
                sqlOperations,
                writerFactory,
                Timestamp(System.currentTimeMillis()),
            ),
        )
    }

    @Test
    fun `selectColumns should return the correct columns`() {
        val result = databricksS3StreamCopier.getSelectColumns(avroSchema)

        assertEquals(expectedSelectColumns, result)

        verify { databricksS3StreamCopier.getSelectColumns(avroSchema) }

        confirmVerified(databricksS3StreamCopier)
    }

    @Test
    fun `createDestinationTable should return the correct SQL`() {
        val createTableSql = slot<String>()

        justRun { jdbcDatabase.execute(capture(createTableSql)) }

        val destinationTable = databricksS3StreamCopier.createDestinationTable()
        val expected = """
        |CREATE OR REPLACE TABLE $catalog.$schemaName.$streamName
        |USING DELTA
        |LOCATION 's3://$bucketName/$bucketPath/$schemaName/$streamName'
        |COMMENT 'Created from stream $streamName'
        |TBLPROPERTIES ('airbyte.destinationSyncMode' = '$mode', delta.autoOptimize.autoCompact = true, delta.autoOptimize.optimizeWrite = true)
        |AS SELECT $expectedSelectColumns FROM $schemaName.$tmpTableName LIMIT 0;""".trimMargin()
            .replace("\n", " ")

        assertEquals(streamName, destinationTable)
        assertEquals(expected, createTableSql.captured)

        verify {
            jdbcDatabase.execute(any<String>())
            databricksS3StreamCopier.createDestinationTable()
            RandomStringUtils.randomAlphabetic(any())
        }

        confirmVerified(jdbcDatabase, databricksS3StreamCopier)
    }

    @Test
    fun `generateMergeStatement should return the correct SQL`() {
        val expectedCopyIntoQuery = """
        |COPY INTO $catalog.$schemaName.$streamName FROM (
        |SELECT
        |$expectedSelectColumns
        |FROM 's3://$bucketName/'
        |)
        |FILEFORMAT = PARQUET
        |PATTERN = ''
        |COPY_OPTIONS ('mergeSchema' = 'false');""".trimMargin().replace("\n", " ")

        val result = databricksS3StreamCopier.generateMergeStatement(streamName)

        assertEquals(expectedCopyIntoQuery, result)

        verify { databricksS3StreamCopier.generateMergeStatement(streamName) }

        confirmVerified(databricksS3StreamCopier)
    }
}
