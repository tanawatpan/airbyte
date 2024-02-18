/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.databricks;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.db.jdbc.DefaultJdbcDatabase;
import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.cdk.integrations.base.AirbyteMessageConsumer;
import io.airbyte.cdk.integrations.destination.StandardNameTransformer;
import io.airbyte.cdk.integrations.destination.jdbc.CopyConsumerFactoryV2;
import io.airbyte.cdk.integrations.destination.jdbc.copy.CopyDestination;
import io.airbyte.integrations.destination.databricks.utils.DatabricksDatabaseUtil;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;

import javax.sql.DataSource;
import java.util.function.Consumer;

import static io.airbyte.integrations.destination.databricks.utils.DatabricksConstants.DATABRICKS_SCHEMA_KEY;

public abstract class DatabricksExternalStorageBaseDestination extends CopyDestination {

    public DatabricksExternalStorageBaseDestination() {
        super(DATABRICKS_SCHEMA_KEY);
    }

    @Override
    public void checkPersistence(JsonNode config) {
        checkPersistence(DatabricksDestinationConfig.get(config).storageConfig());
    }

    protected abstract void checkPersistence(DatabricksStorageConfigProvider databricksConfig);

    @Override
    public AirbyteMessageConsumer getConsumer(final JsonNode config,
                                              final ConfiguredAirbyteCatalog catalog,
                                              final Consumer<AirbyteMessage> outputRecordCollector) {
        final DataSource dataSource = getDataSource(config);
        final DatabricksDestinationConfig databricksConfig = DatabricksDestinationConfig.get(config);
        final DatabricksService databricksService = new DatabricksService(databricksConfig);
        databricksConfig.displayConfiguration();
        return CopyConsumerFactoryV2.create(
                outputRecordCollector,
                dataSource,
                getDatabase(dataSource),
                databricksService,
                getSqlOperations(),
                getNameTransformer(),
                databricksConfig,
                catalog,
                getStreamCopierFactory(),
                databricksConfig.schema());
    }

    protected abstract DatabricksStreamCopierFactory getStreamCopierFactory();

    @Override
    public StandardNameTransformer getNameTransformer() {
        return new DatabricksNameTransformer();
    }

    @Override
    public DataSource getDataSource(final JsonNode config) {
        return DatabricksDatabaseUtil.getDataSource(config);
    }

    @Override
    public JdbcDatabase getDatabase(final DataSource dataSource) {
        return new DefaultJdbcDatabase(dataSource);
    }

    @Override
    public DatabricksSqlOperations getSqlOperations() {
        return new DatabricksSqlOperations();
    }

}