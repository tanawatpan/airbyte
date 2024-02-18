/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.databricks;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.db.jdbc.DefaultJdbcDatabase;
import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.cdk.integrations.base.SerializedAirbyteMessageConsumer;
import io.airbyte.cdk.integrations.destination.jdbc.AbstractJdbcDestination;
import io.airbyte.cdk.integrations.destination.jdbc.JdbcBufferedConsumerFactoryV2;
import io.airbyte.cdk.integrations.destination.jdbc.typing_deduping.JdbcDestinationHandler;
import io.airbyte.cdk.integrations.destination.jdbc.typing_deduping.JdbcSqlGenerator;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.base.destination.typing_deduping.DestinationHandler;
import io.airbyte.integrations.base.destination.typing_deduping.SqlGenerator;
import io.airbyte.integrations.base.destination.typing_deduping.migrators.Migration;
import io.airbyte.integrations.base.destination.typing_deduping.migrators.MinimumDestinationState;
import io.airbyte.integrations.destination.databricks.utils.DatabricksConstants;
import io.airbyte.integrations.destination.databricks.utils.DatabricksDatabaseUtil;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class DatabricksManagedTablesDestination extends AbstractJdbcDestination<MinimumDestinationState> {

    public DatabricksManagedTablesDestination() {
        super(DatabricksConstants.DATABRICKS_DRIVER_CLASS, new DatabricksNameTransformer(), new DatabricksSqlOperations());
    }

    @Override
    public SerializedAirbyteMessageConsumer getSerializedMessageConsumer(final JsonNode config,
                                                                         final ConfiguredAirbyteCatalog catalog,
                                                                         final Consumer<AirbyteMessage> outputRecordCollector) {
        final DataSource dataSource = getDataSource(config);
        final JdbcDatabase database = getDatabase(dataSource);
        final String defaultNamespace = config.get("schema").asText();
        final DatabricksDestinationConfig databricksConfig = DatabricksDestinationConfig.get(config);
        final DatabricksService databricksService = new DatabricksService(databricksConfig);
        databricksConfig.displayConfiguration();
        for (final ConfiguredAirbyteStream stream : catalog.getStreams()) {
            if (StringUtils.isEmpty(stream.getStream().getNamespace())) {
                stream.getStream().setNamespace(defaultNamespace);
            }
        }

        return JdbcBufferedConsumerFactoryV2.createAsync(
                outputRecordCollector,
                dataSource,
                database,
                databricksService,
                getSqlOperations(),
                getNamingResolver(),
                config,
                databricksConfig,
                catalog,
                Optional.of(defaultNamespace));
    }

    @Override
    protected Map<String, String> getDefaultConnectionProperties(final JsonNode config) {
        return Collections.emptyMap();
    }

    @Override
    public JsonNode toJdbcConfig(final JsonNode config) {
        return Jsons.emptyObject();
    }

    @Override
    protected JdbcSqlGenerator getSqlGenerator() {
        return null;
    }

    @Override
    protected JdbcDestinationHandler<MinimumDestinationState> getDestinationHandler(
            String databaseName,
            JdbcDatabase database,
            String rawTableSchema) {
        return null;
    }

    @Override
    protected List<Migration<MinimumDestinationState>> getMigrations(JdbcDatabase database, String databaseName, SqlGenerator sqlGenerator, DestinationHandler<MinimumDestinationState> destinationHandler) {
        return null;
    }

    @Override
    public JdbcDatabase getDatabase(final DataSource dataSource) {
        return new DefaultJdbcDatabase(dataSource);
    }

    @Override
    public DatabricksSqlOperations getSqlOperations() {
        return new DatabricksSqlOperations();
    }

    @Override
    public DataSource getDataSource(final JsonNode config) {
        return DatabricksDatabaseUtil.getDataSource(config);
    }

}
