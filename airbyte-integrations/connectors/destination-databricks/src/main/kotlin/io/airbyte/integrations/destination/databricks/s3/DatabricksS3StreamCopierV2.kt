package io.airbyte.integrations.destination.databricks.s3

import com.amazonaws.services.s3.AmazonS3
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.cdk.db.jdbc.JdbcDatabase
import io.airbyte.cdk.integrations.destination.StandardNameTransformer
import io.airbyte.cdk.integrations.destination.jdbc.SqlOperations
import io.airbyte.cdk.integrations.destination.s3.S3DestinationConfig
import io.airbyte.cdk.integrations.destination.s3.parquet.S3ParquetFormatConfig
import io.airbyte.cdk.integrations.destination.s3.parquet.S3ParquetWriter
import io.airbyte.cdk.integrations.destination.s3.writer.S3WriterFactory
import io.airbyte.integrations.destination.databricks.DatabricksDestinationConfig
import io.airbyte.integrations.destination.databricks.DatabricksStreamCopier
import io.airbyte.integrations.destination.databricks.utils.DatabricksConstants
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.DestinationSyncMode
import java.sql.Timestamp
import java.util.*
import org.apache.avro.Schema
import org.slf4j.LoggerFactory

class DatabricksS3StreamCopierV2(
    private val stagingFolder: String,
    private val catalog: String,
    private val schema: String,
    private val configuredStream: ConfiguredAirbyteStream,
    private val s3Client: AmazonS3?,
    private val database: JdbcDatabase,
    private val databricksConfig: DatabricksDestinationConfig,
    private val nameTransformer: StandardNameTransformer,
    private val sqlOperations: SqlOperations,
    private val writerFactory: S3WriterFactory,
    private val uploadTime: Timestamp
) : DatabricksStreamCopier(
    stagingFolder,
    catalog,
    schema,
    configuredStream,
    database,
    databricksConfig,
    nameTransformer,
    sqlOperations,
) {

    companion object {
        private val logger = LoggerFactory.getLogger(DatabricksS3StreamCopierV2::class.java)
        private val mapper = ObjectMapper()

        fun getStagingS3DestinationConfig(
            config: S3DestinationConfig?,
            stagingFolder: String
        ): S3DestinationConfig {
            return S3DestinationConfig.create(config)
                .withBucketPath("${config?.bucketPath}/$stagingFolder")
                .withFormatConfig(S3ParquetFormatConfig(mapper.createObjectNode()))
                .withFileNamePattern(config?.fileNamePattern.orEmpty())
                .get()
        }
    }

    private val s3Config: S3DestinationConfig? =
        databricksConfig.storageConfig?.s3DestinationConfigOrThrow
    private val parquetWriter: S3ParquetWriter = writerFactory.create(
        getStagingS3DestinationConfig(s3Config, stagingFolder),
        s3Client,
        configuredStream,
        uploadTime,
    ) as S3ParquetWriter
    private val selectColumns by lazy { getSelectColumns(parquetWriter.schema) }

    init {
        logger.info("[Stream $streamName] Parquet schema: ${parquetWriter.schema}")
        logger.info("[Stream $streamName] Tmp table $tmpTableName location: $tmpTableLocation")
        logger.info("[Stream $streamName] Data table $destTableName location: $destTableLocation")
        parquetWriter.initialize()
    }

    fun getSelectColumns(schema: Schema): String {
        return schema.fields.joinToString(", ") { field ->
            val fieldName = field.name()
            val fieldType = field.schema()

            // Unpack nested Time/Date types
            when (fieldType.type) {
                Schema.Type.UNION -> {
                    fieldType.types
                        .filter { it.type != Schema.Type.NULL }
                        .indexOfFirst { schema ->
                            schema.logicalType?.name?.run {
                                contains("time", ignoreCase = true) || contains(
                                    "date",
                                    ignoreCase = true,
                                )
                            } ?: false
                        }.run {
                            if (this > -1 && fieldType.types.size in 2..3)
                                "CAST(${fieldName}.member${this} AS TIMESTAMP) AS $fieldName"
                            else fieldName
                        }
                }

                else -> fieldName
            }
        }
    }

    override fun createDestinationTable(): String {
        logger.info("[Stream $streamName] Creating destination table if it does not exist: $destTableName")

        val createStatement = when (destinationSyncMode) {
            DestinationSyncMode.OVERWRITE -> "CREATE OR REPLACE TABLE"
            else -> "CREATE TABLE IF NOT EXISTS"
        }

        val syncMode = destinationSyncMode.value()
        val defaultTblProperties =
            DatabricksConstants.DEFAULT_TBL_PROPERTIES.sorted().joinToString(", ")

        val createTable = """
        |$createStatement $catalogName.$schemaName.$destTableName
        |USING DELTA
        |LOCATION '$destTableLocation'
        |COMMENT 'Created from stream $streamName'
        |TBLPROPERTIES ('airbyte.destinationSyncMode' = '$syncMode', $defaultTblProperties)
        |AS SELECT $selectColumns FROM $schemaName.$tmpTableName LIMIT 0;""".trimMargin()
            .replace("\n", " ")
        logger.info(createTable)
        database.execute(createTable)

        return destTableName
    }

    override fun generateMergeStatement(destTableName: String?) = """
        |COPY INTO $catalogName.$schemaName.$destTableName FROM (
        |SELECT
        |$selectColumns
        |FROM '$tmpTableLocation'
        |)
        |FILEFORMAT = PARQUET
        |PATTERN = '${parquetWriter.outputFilename}'
        |COPY_OPTIONS ('mergeSchema' = '${databricksConfig.enableSchemaEvolution}');""".trimMargin()
        .replace("\n", " ")

    override fun prepareStagingFile() = "${s3Config?.bucketPath}/$stagingFolder"

    override fun write(id: UUID, recordMessage: AirbyteRecordMessage, fileName: String) {
        parquetWriter.write(id, recordMessage)
    }

    override fun closeStagingUploader(hasFailed: Boolean) {
        parquetWriter.close(hasFailed)
    }

    override fun getTmpTableLocation() =
        "s3://${s3Config?.bucketName}/${parquetWriter.outputPrefix}"

    override fun getDestTableLocation() =
        "s3://${s3Config?.bucketName}/${s3Config?.bucketPath}/$schemaName/$streamName"

    override fun getCreateTempTableStatement() =
        "CREATE TABLE $catalogName.$schemaName.$tmpTableName USING parquet LOCATION '${tmpTableLocation}';"

    override fun deleteStagingFile() {
        logger.info("[Stream $streamName] Deleting staging file: ${parquetWriter.outputFilePath}")
        s3Client?.deleteObject(s3Config?.bucketName, parquetWriter.outputFilePath)
    }

    override fun closeNonCurrentStagingFileWriters() {
        parquetWriter.close(false)
    }

    override fun getCurrentFile() = ""
}
