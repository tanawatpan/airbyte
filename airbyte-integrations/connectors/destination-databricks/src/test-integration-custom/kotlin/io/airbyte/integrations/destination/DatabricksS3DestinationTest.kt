package io.airbyte.integrations.destination

import com.amazonaws.services.s3.model.DeleteObjectsRequest
import io.airbyte.cdk.integrations.base.DatabricksIntegrationTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

class DatabricksS3DestinationTest : DatabricksIntegrationTest() {
    override val configFile = "s3_destination_config.json"

    private val s3Config by lazy { databricksConfig.storageConfig!!.s3DestinationConfigOrThrow }
    private val s3Client by lazy { s3Config.s3Client }

    @Test
    fun testBasicSync() {
        testSync(
            catalogFile = "exchange_rate_catalog.json",
            messageFile = "exchange_rate_messages.txt",
        )
    }

    @Test
    fun testEdgeCaseSync() {
        testSync(
            catalogFile = "edge_case_catalog.json",
            messageFile = "edge_case_messages.txt",
        )
    }

    @AfterEach
    override fun tearDown() {
        val keysToDelete = s3Client
            .listObjects(s3Config.bucketName, s3Config.bucketPath)
            .objectSummaries
            .map { DeleteObjectsRequest.KeyVersion(it.key) }

        if (keysToDelete.isNotEmpty()) {
            logger.info("Tearing down test bucket path: ${s3Config.bucketName}/${s3Config.bucketPath}")
            val result = s3Client.deleteObjects(
                DeleteObjectsRequest(s3Config.bucketName).withKeys(keysToDelete),
            )
            logger.info("Deleted ${result.deletedObjects.size} file(s).")
        }
        s3Client.shutdown()
        super.tearDown()
    }

    override fun getSelectColumns(columns: Sequence<Pair<String, String>>) =
        columns.map { (column, type) ->
            val columnName = nameTransformer.getIdentifier(column)
            val columnType = type.lowercase().trim()
            if (columnType.contains("date") || columnType.contains("time")) {
                "date_format(`$columnName`, \"yyyy-MM-dd'T'HH:mm:ss'Z'\") AS `$column`"
            } else {
                "`$columnName` AS `$column`"
            }
        }.joinToString(", ")
}
