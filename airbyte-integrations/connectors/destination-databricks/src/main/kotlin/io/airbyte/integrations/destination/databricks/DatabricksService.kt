package io.airbyte.integrations.destination.databricks

import com.fasterxml.jackson.annotation.JsonProperty
import io.airbyte.commons.json.Jsons
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class DatabricksService(private val config: DatabricksDestinationConfig) {
    data class Query(
        @JsonProperty("query_id")
        val id: String = "",
        @JsonProperty("status")
        val status: String = "",
        @JsonProperty("query_start_time_ms")
        val startTimestamp: Long = 0,
        @JsonProperty("query_end_time_ms")
        val endTimestamp: Long = 0,
    )

    data class ListQueriesResponse(
        @JsonProperty("next_page_token")
        val nextPageToken: String? = "",
        @JsonProperty("has_next_page")
        val hasNextPage: Boolean = false,
        @JsonProperty("res")
        val queries: List<Query>? = Collections.emptyList()
    )

    private val logger: Logger = LoggerFactory.getLogger(DatabricksService::class.java)
    private val client = HttpClient.newHttpClient()
    private val apiVersion = "2.0"

    fun stopClusterIfIdle() {
        try {
            logger.info("Checking if the cluster is idle and can be stopped.")
            when {
                !config.enableExperimentalFeatures -> logger.info("Skipping cluster stop as experimental features are disabled.")
                !config.isWarehouse() -> logger.info("Skipping cluster stop as it is not a warehouse.")
                anyRunningQueries() -> logger.info("Skipping cluster stop as there are running or recently executed queries.")
                else -> stopCluster()
            }
        } catch (e: Exception) {
            logger.error("Error occurred while attempting to stop the cluster.", e)
        }
    }

    private fun anyRunningQueries(): Boolean {
        val now = System.currentTimeMillis()
        val id = config.getId()

        val runningQueries = listQueries(
            queryParams = mapOf(
                "max_results" to "100",
            ),
            payload = """
                {
                  "filter_by": {
                    "statuses": ["QUEUED", "RUNNING"],
                    "warehouse_ids": ["$id"],
                    "query_start_time_range": {
                      "start_time_ms": ${now - 1000 * 60 * 60 * 2 * 1}
                    }
                  }
                }
            """.trimIndent(),
        )
        logger.info("Total running queries: ${runningQueries.size}")

        return runningQueries.isNotEmpty()
    }

    fun listQueries(
        queryParams: Map<String, String> = emptyMap(),
        payload: String = ""
    ): List<Query> {
        val initialResponse = listQueriesInternal(queryParams, payload)
        var nextPageToken = initialResponse.nextPageToken.orEmpty()
        return when {
            nextPageToken.isEmpty() -> initialResponse.queries.orEmpty()
            else -> generateSequence { nextPageToken }
                .takeWhile { it.isNotEmpty() }
                .flatMap { token ->
                    val response = listQueriesInternal(queryParams + mapOf("page_token" to token))
                    nextPageToken = response.nextPageToken.orEmpty()
                    response.queries.orEmpty()
                }.toList()
        }
    }

    private fun listQueriesInternal(
        queryParams: Map<String, String> = emptyMap(),
        payload: String = ""
    ): ListQueriesResponse {
        val operationPath = "api/$apiVersion/sql/history/queries"
        val params = queryParams.toQueryParams()
        val uri =
            URI.create("https://${config.serverHostname}:${config.port}/$operationPath$params")
        val body = when (payload) {
            "" -> HttpRequest.BodyPublishers.noBody()
            else -> HttpRequest.BodyPublishers.ofString(payload)
        }

        val request = HttpRequest.newBuilder(uri)
            .header("Content-Type", "application/json")
            .header("Authorization", "Bearer ${config.personalAccessToken}")
            .method("GET", body)
            .build()

        return client.send(request, HttpResponse.BodyHandlers.ofString()).run {
            if (statusCode() !in 200..299)
                throw RuntimeException("Failed to list queries. - Response code: ${statusCode()} - Response body: ${body()}")
            Jsons.deserialize(body(), ListQueriesResponse::class.java)
        }
    }

    fun stopCluster() {
        val id = config.getId()
        val isWarehouse = config.isWarehouse()

        val operationPath = when (isWarehouse) {
            true -> "api/$apiVersion/sql/warehouses/$id/stop"
            else -> "api/$apiVersion/clusters/delete"
        }
        val uri = URI.create("https://${config.serverHostname}:${config.port}/$operationPath")
        val body = when (isWarehouse) {
            true -> HttpRequest.BodyPublishers.noBody()
            else -> HttpRequest.BodyPublishers.ofString("{\"cluster_id\": \"$id\"}")
        }
        val request = HttpRequest.newBuilder(uri)
            .header("Content-Type", "application/json")
            .header("Authorization", "Bearer ${config.personalAccessToken}")
            .POST(body)
            .build()

        client.send(request, HttpResponse.BodyHandlers.ofString()).run {
            if (statusCode() in 200..299)
                logger.info("Cluster stopped successfully.")
            else
                logger.error("Failed to stop the cluster. - Response code: ${statusCode()} - Response body: ${body()}")
        }
    }

    private fun Map<String, String>.toQueryParams(): String = when {
        isNotEmpty() -> entries.joinToString("&") { (key, value) ->
            URLEncoder.encode(key, StandardCharsets.UTF_8) + "=" +
                URLEncoder.encode(value, StandardCharsets.UTF_8)
        }.run { "?$this" }

        else -> ""
    }
}
