package io.airbyte.integrations.destination.databricks.utils

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.db.factory.DataSourceFactory
import io.airbyte.cdk.db.factory.DatabaseDriver
import io.airbyte.integrations.destination.databricks.DatabricksDestinationConfig
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import java.time.Duration
import javax.sql.DataSource

object DatabricksDatabaseUtil {

    @JvmStatic
    fun getDatabricksConnectionString(databricksConfig: DatabricksDestinationConfig): String {
        return String.format(
            DatabaseDriver.DATABRICKS.urlFormatString,
            databricksConfig.serverHostname,
            databricksConfig.port,
            databricksConfig.httpPath
        )
    }

    @JvmStatic
    fun getDataSource(config: JsonNode): DataSource {
        val property = mutableMapOf<String, String>()
        config.get(DatabricksConstants.DATABRICKS_CATALOG_KEY)?.apply {
            property[DatabricksConstants.DATABRICKS_CATALOG_JDBC_KEY] = asText()
        }
        config.get(DatabricksConstants.DATABRICKS_SCHEMA_KEY)?.apply {
            property[DatabricksConstants.DATABRICKS_SCHEMA_JDBC_KEY] = asText()
        }
        val databricksConfig = DatabricksDestinationConfig[config]
        return DataSourceFactory.create(
            DatabricksConstants.DATABRICKS_USERNAME,
            databricksConfig.personalAccessToken,
            DatabricksConstants.DATABRICKS_DRIVER_CLASS,
            getDatabricksConnectionString(databricksConfig),
            mapOf("EnableArrow" to "0") + property,
            Duration.ofSeconds(600)
        )
    }

    @JvmStatic
    fun getDataSource(databricksConfig: DatabricksDestinationConfig): DataSource {
        return DataSourceFactory.create(
            DatabricksConstants.DATABRICKS_USERNAME,
            databricksConfig.personalAccessToken,
            DatabricksConstants.DATABRICKS_DRIVER_CLASS,
            getDatabricksConnectionString(databricksConfig),
            mapOf("EnableArrow" to "0"),
            Duration.ofSeconds(600)
        )
    }

    @JvmStatic
    fun getDslContext(dataSource: DataSource): DSLContext {
        return DSL.using(dataSource, SQLDialect.DEFAULT)
    }
}