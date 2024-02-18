package io.airbyte.cdk.integrations.base

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.resources.MoreResources
import io.airbyte.integrations.destination.databricks.DatabricksDestinationConfig
import io.airbyte.integrations.destination.databricks.DatabricksService
import java.util.concurrent.atomic.AtomicBoolean
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class DatabricksIntegrationTestExtension : BeforeAllCallback,
    ExtensionContext.Store.CloseableResource {
    val logger: Logger = LoggerFactory.getLogger(DatabricksIntegrationTestExtension::class.java)

    companion object {
        val databricksConfig = DatabricksDestinationConfig[
            Jsons.deserialize(MoreResources.readResource("managed_tables_config.json")),
        ]
        private val started = AtomicBoolean(false)
    }

    override fun beforeAll(context: ExtensionContext?) {
        if (started.compareAndSet(false, true)) {
            logger.info("Before all tests")
            // The following line registers a callback hook when the root test context is shut down
            context?.root?.getStore(ExtensionContext.Namespace.GLOBAL)
                ?.put("DatabricksIntegrationTestExtension", this)
        }
    }

    override fun close() {
        logger.info("After all tests")
        DatabricksService(databricksConfig).stopCluster()
    }
}
