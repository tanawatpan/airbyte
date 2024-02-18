package io.airbyte.integrations.destination

import io.airbyte.cdk.db.factory.DataSourceFactory
import io.airbyte.cdk.integrations.base.DatabricksIntegrationTest
import io.airbyte.integrations.destination.databricks.DatabricksService
import kotlin.test.assertEquals
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

class DatabricksServiceTest : DatabricksIntegrationTest() {
    override val configFile: String = "managed_tables_config.json"
    private val service by lazy {
        DatabricksService(databricksConfig)
    }

    @Test
    fun testListRecentQueries() {
        val now = System.currentTimeMillis()
        val result = database.query { ctx -> ctx.fetch("SELECT 1") }.map { it[0, Int::class.java] }
        assertEquals(1, result.size)

        val runningQueries = service.listQueries(
            queryParams = mapOf(
                "max_results" to "1",
            ),
            payload = """
                {
                  "filter_by": {
                    "statuses": ["FINISHED"],
                    "warehouse_ids": ["${databricksConfig.getId()}"],
                    "query_start_time_range": {
                      "start_time_ms": $now
                    }
                  }
                }
            """.trimIndent(),
        )
        logger.info("Total running queries: ${runningQueries.size}")

        assertEquals(1, runningQueries.size)
    }

    @Test
    fun testListQueriesWithMultiplePages() {
        val now = System.currentTimeMillis()
        val result = (1..10).flatMap {
            database.query { ctx -> ctx.fetch("SELECT $it") }.map { it[0, Int::class.java] }
        }
        assertEquals(10, result.size)

        val queries = service.listQueries(
            queryParams = mapOf(
                "max_results" to "2",
            ),
            payload = """
                {
                  "filter_by": {
                    "warehouse_ids": ["${databricksConfig.getId()}"],
                    "query_start_time_range": {
                      "start_time_ms": $now
                    }
                  }
                }
            """.trimIndent(),
        )
        assert(queries.size >= 8)
    }

    @AfterEach
    override fun tearDown() {
        DataSourceFactory.close(dataSource)
    }
}
