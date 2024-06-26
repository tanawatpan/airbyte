/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.databricks.azure;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.integrations.destination.jdbc.copy.azure.AzureBlobStorageConfig;
import io.airbyte.integrations.destination.databricks.DatabricksStorageConfigProvider;

public class DatabricksAzureBlobStorageConfigProvider extends DatabricksStorageConfigProvider {

  private final AzureBlobStorageConfig azureConfig;

  public DatabricksAzureBlobStorageConfigProvider(JsonNode config) {
    this.azureConfig = AzureBlobStorageConfig.Companion.getAzureBlobConfig(config);
  }

  @Override
  public AzureBlobStorageConfig getAzureBlobStorageConfigOrThrow() {
    return azureConfig;
  }

}
