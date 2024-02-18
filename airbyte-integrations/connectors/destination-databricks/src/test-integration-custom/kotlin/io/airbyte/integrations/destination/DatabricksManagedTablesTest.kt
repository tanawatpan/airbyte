package io.airbyte.integrations.destination

import io.airbyte.cdk.integrations.base.DatabricksIntegrationTest
import org.junit.jupiter.api.Test

class DatabricksManagedTablesTest : DatabricksIntegrationTest() {
    override val configFile = "managed_tables_config.json"

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
}