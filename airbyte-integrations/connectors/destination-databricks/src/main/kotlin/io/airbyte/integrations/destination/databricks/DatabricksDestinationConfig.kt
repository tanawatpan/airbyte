package io.airbyte.integrations.destination.databricks

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.base.Preconditions
import io.airbyte.integrations.destination.databricks.utils.DatabricksConstants.DATABRICKS_CATALOG_KEY
import io.airbyte.integrations.destination.databricks.utils.DatabricksConstants.DATABRICKS_DATA_SOURCE_KEY
import io.airbyte.integrations.destination.databricks.utils.DatabricksConstants.DATABRICKS_DEFAULT_VACUUM_RETENTION_HOURS
import io.airbyte.integrations.destination.databricks.utils.DatabricksConstants.DATABRICKS_DEFAULT_VACUUM_RETENTION_HOURS_KEY
import io.airbyte.integrations.destination.databricks.utils.DatabricksConstants.DATABRICKS_ENABLE_EXPERIMENTAL_FEATURES_KEY
import io.airbyte.integrations.destination.databricks.utils.DatabricksConstants.DATABRICKS_ENABLE_OPTIMIZE_TABLE_KEY
import io.airbyte.integrations.destination.databricks.utils.DatabricksConstants.DATABRICKS_ENABLE_SCHEMA_EVOLUTION_KEY
import io.airbyte.integrations.destination.databricks.utils.DatabricksConstants.DATABRICKS_ENABLE_VACUUM_TABLE_KEY
import io.airbyte.integrations.destination.databricks.utils.DatabricksConstants.DATABRICKS_HTTP_PATH_KEY
import io.airbyte.integrations.destination.databricks.utils.DatabricksConstants.DATABRICKS_PERSONAL_ACCESS_TOKEN_KEY
import io.airbyte.integrations.destination.databricks.utils.DatabricksConstants.DATABRICKS_PORT_KEY
import io.airbyte.integrations.destination.databricks.utils.DatabricksConstants.DATABRICKS_PURGE_STAGING_DATA_KEY
import io.airbyte.integrations.destination.databricks.utils.DatabricksConstants.DATABRICKS_SCHEMA_KEY
import io.airbyte.integrations.destination.databricks.utils.DatabricksConstants.DATABRICKS_SERVER_HOSTNAME_KEY
import org.slf4j.LoggerFactory

@JvmRecord
data class DatabricksDestinationConfig(
    val serverHostname: String,
    val httpPath: String,
    val port: String,
    val personalAccessToken: String,
    val catalog: String,
    val schema: String,
    val isPurgeStagingData: Boolean,
    val enableSchemaEvolution: Boolean,
    val storageConfig: DatabricksStorageConfigProvider?,
    val enableOptimizeTable: Boolean = false,
    val enableVacuumTable: Boolean = false,
    val vacuumRetainHours: Int = DATABRICKS_DEFAULT_VACUUM_RETENTION_HOURS,
    val enableExperimentalFeatures: Boolean = false
) {
    companion object {
        private val logger = LoggerFactory.getLogger(DatabricksDestinationConfig::class.java)

        const val DEFAULT_DATABRICKS_PORT = "443"

        const val DEFAULT_DATABASE_SCHEMA = "default"

        const val DEFAULT_CATALOG = "hive_metastore"

        const val DEFAULT_PURGE_STAGING_DATA = true

        const val DEFAULT_ENABLE_SCHEMA_EVOLUTION = false

        const val DEFAULT_ENABLE_OPTIMIZE_TABLE = false

        const val DEFAULT_ENABLE_VACUUM_TABLE = false

        @JvmStatic
        operator fun get(config: JsonNode): DatabricksDestinationConfig {
            Preconditions.checkArgument(
                config.has("accept_terms") && config.get("accept_terms").asBoolean(),
                "You must agree to the Databricks JDBC Terms & Conditions to use this connector",
            )

            val databricksConfig = DatabricksDestinationConfig(
                serverHostname = config[DATABRICKS_SERVER_HOSTNAME_KEY].asText(),
                httpPath = config[DATABRICKS_HTTP_PATH_KEY].asText(),
                port = config[DATABRICKS_PORT_KEY]?.asText() ?: DEFAULT_DATABRICKS_PORT,
                personalAccessToken = config[DATABRICKS_PERSONAL_ACCESS_TOKEN_KEY].asText(),
                catalog = config[DATABRICKS_CATALOG_KEY]?.asText() ?: DEFAULT_CATALOG,
                schema = config[DATABRICKS_SCHEMA_KEY]?.asText() ?: DEFAULT_DATABASE_SCHEMA,
                isPurgeStagingData = config[DATABRICKS_PURGE_STAGING_DATA_KEY]?.asBoolean()
                    ?: DEFAULT_PURGE_STAGING_DATA,
                enableSchemaEvolution = config[DATABRICKS_ENABLE_SCHEMA_EVOLUTION_KEY]?.asBoolean()
                    ?: DEFAULT_ENABLE_SCHEMA_EVOLUTION,
                enableOptimizeTable = config[DATABRICKS_ENABLE_OPTIMIZE_TABLE_KEY]?.asBoolean()
                    ?: DEFAULT_ENABLE_OPTIMIZE_TABLE,
                enableVacuumTable = config[DATABRICKS_ENABLE_VACUUM_TABLE_KEY]?.asBoolean()
                    ?: DEFAULT_ENABLE_VACUUM_TABLE,
                vacuumRetainHours = System.getenv(DATABRICKS_DEFAULT_VACUUM_RETENTION_HOURS_KEY)
                    ?.toIntOrNull()
                    ?: DATABRICKS_DEFAULT_VACUUM_RETENTION_HOURS,
                storageConfig = DatabricksStorageConfigProvider.getDatabricksStorageConfig(config[DATABRICKS_DATA_SOURCE_KEY]),
                enableExperimentalFeatures = config[DATABRICKS_ENABLE_EXPERIMENTAL_FEATURES_KEY]?.asBoolean()
                    ?: false,
            )

            return databricksConfig
        }
    }

    fun getId(): String = httpPath.split("/").last()
    fun isWarehouse(): Boolean = httpPath.contains("warehouse")

    fun displayConfiguration() {
        logger.info("isPurgeStagingData: $isPurgeStagingData")
        logger.info("enableSchemaEvolution: $enableSchemaEvolution")
        logger.info("enableOptimizeTable: $enableOptimizeTable")
        logger.info("enableVacuumTable: $enableVacuumTable")
        logger.info("enableExperimentalFeatures: $enableExperimentalFeatures")
    }
}
