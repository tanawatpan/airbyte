package io.airbyte.integrations.destination.databricks.s3


import com.amazonaws.services.s3.AmazonS3
import io.airbyte.cdk.db.jdbc.JdbcDatabase
import io.airbyte.cdk.integrations.destination.StandardNameTransformer
import io.airbyte.cdk.integrations.destination.jdbc.SqlOperations
import io.airbyte.cdk.integrations.destination.jdbc.copy.StreamCopier
import io.airbyte.cdk.integrations.destination.jdbc.copy.StreamCopierFactory
import io.airbyte.cdk.integrations.destination.s3.S3DestinationConfig
import io.airbyte.cdk.integrations.destination.s3.writer.ProductionWriterFactory
import io.airbyte.integrations.destination.databricks.DatabricksDestinationConfig
import io.airbyte.integrations.destination.databricks.DatabricksSqlOperations
import io.airbyte.integrations.destination.databricks.DatabricksStreamCopierFactory
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import java.sql.Timestamp

open class DatabricksS3StreamCopierFactoryV2 : DatabricksStreamCopierFactory {

    override fun create(
        configuredSchema: String?,
        config: DatabricksDestinationConfig?,
        stagingFolder: String?,
        configuredStream: ConfiguredAirbyteStream?,
        nameTransformer: StandardNameTransformer?,
        db: JdbcDatabase?,
        sqlOperations: SqlOperations?
    ): StreamCopier? {
        return try {
            val stream: AirbyteStream =
                configuredStream?.stream ?: throw IllegalStateException("Stream is not set")
            val catalogName: String =
                config?.catalog ?: throw IllegalStateException("Catalog name is not set")
            val schema: String =
                StreamCopierFactory.getSchema(
                    stream.namespace,
                    configuredSchema.orEmpty(),
                    nameTransformer ?: StandardNameTransformer(),
                ).orEmpty()

            val s3Config: S3DestinationConfig? = config.storageConfig?.s3DestinationConfigOrThrow
            val s3Client: AmazonS3? = s3Config?.getS3Client()
            val writerFactory = ProductionWriterFactory()
            val uploadTimestamp = Timestamp(System.currentTimeMillis())
            DatabricksS3StreamCopierV2(
                stagingFolder.orEmpty(),
                catalogName,
                schema,
                configuredStream,
                s3Client,
                db ?: throw IllegalStateException("Database is not set"),
                config,
                nameTransformer ?: StandardNameTransformer(),
                sqlOperations ?: DatabricksSqlOperations(),
                writerFactory,
                uploadTimestamp,
            )
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }
}
