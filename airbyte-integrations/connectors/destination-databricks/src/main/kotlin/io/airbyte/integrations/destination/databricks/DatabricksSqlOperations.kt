package io.airbyte.integrations.destination.databricks

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import io.airbyte.cdk.db.jdbc.JdbcDatabase
import io.airbyte.cdk.integrations.base.JavaBaseConstants
import io.airbyte.cdk.integrations.destination.async.partial_messages.PartialAirbyteMessage
import io.airbyte.cdk.integrations.destination.jdbc.JdbcSqlOperations
import io.airbyte.cdk.integrations.destination.jdbc.SqlOperationsUtils
import io.airbyte.protocol.models.v0.DestinationSyncMode
import java.sql.SQLException

class DatabricksSqlOperations : JdbcSqlOperations() {
    @Throws(Exception::class)
    override fun executeTransaction(database: JdbcDatabase, queries: List<String>) =
        queries.flatMap { it.split(";") }.filter { it.isNotBlank() }.forEach {
            LOGGER.info(it)
            database.execute(it)
        }

    @Throws(SQLException::class)
    override fun dropTableIfExists(database: JdbcDatabase, schemaName: String, tableName: String) {
        try {
            database.execute(String.format("DROP TABLE IF EXISTS %s.%s;", schemaName, tableName))
        } catch (e: SQLException) {
            throw checkForKnownConfigExceptions(e).orElseThrow { e }
        }
    }

    @Throws(SQLException::class)
    public override fun insertRecordsInternal(
        database: JdbcDatabase,
        records: List<PartialAirbyteMessage>,
        schemaName: String,
        tmpTableName: String
    ) {
        LOGGER.info("actual size of batch: {}", records.size)
        val insertQueryComponent = String.format(
            "INSERT INTO %s.%s (%s, %s, %s) VALUES\n",
            schemaName,
            tmpTableName,
            JavaBaseConstants.COLUMN_NAME_AB_ID,
            JavaBaseConstants.COLUMN_NAME_DATA,
            JavaBaseConstants.COLUMN_NAME_EMITTED_AT,
        )
        val recordQueryComponent = "(?, ?, ?),\n"
        SqlOperationsUtils.insertRawRecordsInSingleQuery(
            insertQueryComponent,
            recordQueryComponent,
            database,
            records,
        )
    }

    @Throws(SQLException::class)
    override fun insertRecordsInternalV2(
        jdbcDatabase: JdbcDatabase,
        list: List<PartialAirbyteMessage>,
        s: String,
        s1: String
    ) = Unit

    override fun createTableQuery(
        database: JdbcDatabase,
        schemaName: String,
        tableName: String
    ): String {
        return String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s (%s STRING, %s STRING, %s TIMESTAMP);",
            schemaName, tableName,
            JavaBaseConstants.COLUMN_NAME_AB_ID,
            JavaBaseConstants.COLUMN_NAME_DATA,
            JavaBaseConstants.COLUMN_NAME_EMITTED_AT,
        )
    }

    @Throws(SQLException::class)
    fun createTableIfNotExists(
        database: JdbcDatabase,
        catalog: String,
        schemaName: String,
        tableName: String
    ) {
        try {
            String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s.%s (%s STRING, %s STRING, %s TIMESTAMP);",
                catalog, schemaName, tableName,
                JavaBaseConstants.COLUMN_NAME_AB_ID,
                JavaBaseConstants.COLUMN_NAME_DATA,
                JavaBaseConstants.COLUMN_NAME_EMITTED_AT,
            ).apply {
                LOGGER.info(this)
                database.execute(this)
            }
        } catch (e: SQLException) {
            throw checkForKnownConfigExceptions(e).orElseThrow { e }
        }
    }

    @Throws(SQLException::class)
    fun dropTableIfExists(
        database: JdbcDatabase,
        catalog: String,
        schemaName: String,
        tableName: String
    ) {
        try {
            "DROP TABLE IF EXISTS $catalog.$schemaName.$tableName;".apply {
                LOGGER.info(this)
                database.execute(this)
            }
        } catch (e: SQLException) {
            throw checkForKnownConfigExceptions(e).orElseThrow { e }
        }
    }

    @Throws(SQLException::class)
    fun createSchemaIfNotExists(
        database: JdbcDatabase,
        catalog: String,
        schemaName: String
    ) {
        try {
            "CREATE SCHEMA IF NOT EXISTS $catalog.$schemaName".apply {
                LOGGER.info(this)
                database.execute(this)
            }
        } catch (e: SQLException) {
            throw checkForKnownConfigExceptions(e).orElseThrow { e }
        }
    }

    fun getTableSchemaString(
        database: JdbcDatabase,
        catalog: String,
        schemaName: String,
        tmpTableName: String
    ): String {
        val queryResult = """
            |SELECT DISTINCT
            |schema_of_json(${JavaBaseConstants.COLUMN_NAME_DATA}) as schema_json
            |FROM $catalog.$schemaName.$tmpTableName
            |ORDER BY length(schema_json) DESC 
            |LIMIT 1000 ;""".trimMargin().run {
            database.bufferedResultSetQuery(
                { connection -> connection.createStatement().executeQuery(this) },
                { it.getString("schema_json") },
            )
        }

        return jsonToSchemaString(mergeSchemas(queryResult))
    }

    fun getUnpackJsonQuery(
        catalog: String,
        namespace: String,
        tmpTableName: String,
        schemaString: String,
        isOrdered: Boolean = true
    ): String {
        val emittedAt = JavaBaseConstants.COLUMN_NAME_EMITTED_AT
        return """
            |WITH CTE AS (
            | SELECT $emittedAt, from_json( ${JavaBaseConstants.COLUMN_NAME_DATA}, "$schemaString" ) as data
            | FROM $catalog.$namespace.$tmpTableName
            |)
            |SELECT $emittedAt, data.*
            |FROM CTE """.trimMargin()
            .replace("\n", " ") + if (isOrdered) "ORDER BY $emittedAt ASC" else ""
    }

    fun optimizeTable(
        databricksConfig: DatabricksDestinationConfig,
        namespace: String,
        tableName: String,
        jdbc: JdbcDatabase,
    ) {
        if (databricksConfig.enableOptimizeTable) {
            "OPTIMIZE ${databricksConfig.catalog}.$namespace.$tableName;".apply {
                LOGGER.info(this)
                jdbc.execute(this)
            }
        }
    }

    fun vacuumTable(
        databricksConfig: DatabricksDestinationConfig,
        namespace: String,
        tableName: String,
        jdbc: JdbcDatabase,
    ) {
        if (databricksConfig.enableVacuumTable) {
            "VACUUM ${databricksConfig.catalog}.$namespace.$tableName RETAIN ${databricksConfig.vacuumRetainHours} HOURS;".apply {
                LOGGER.info(this)
                jdbc.execute(this)
            }
        }
    }

    fun finalizeTableQuery(
        syncMode: DestinationSyncMode,
        srcSchema: Map<String, String>,
        dstSchema: Map<String, String>,
        catalog: String,
        schemaName: String,
        srcTableName: String,
        dstTableName: String
    ): String {
        val srcColumns = srcSchema.map { it.key }
        val dstColumns = dstSchema.map { it.key }.filter { !it.contains("_airbyte") }
        val allColumns = dstColumns.union(srcColumns)

        val srcSchemaString =
            srcSchema.entries.joinToString(",", "STRUCT<", ">") { (n, t) -> "`$n`:$t" }

        val emittedAt = JavaBaseConstants.COLUMN_NAME_EMITTED_AT
        val dstSelect = allColumns.joinToString(", ") { srcCol ->
            when {
                dstSchema.containsKey(srcCol) -> srcCol
                else -> "CAST(NULL AS ${srcSchema[srcCol]}) as $srcCol"
            }
        }

        val (queryAction, additionalSelect) = when (syncMode) {
            DestinationSyncMode.OVERWRITE -> "CREATE OR REPLACE TABLE $catalog.$schemaName.$dstTableName AS" to ";"
            else -> {
                // Destination has fewer columns
                if (dstSchema.size < srcSchema.size)
                    "CREATE OR REPLACE TABLE $catalog.$schemaName.$dstTableName AS" to "UNION ALL SELECT $emittedAt, $dstSelect FROM $catalog.$schemaName.$dstTableName ;"
                // Schemas match Or Source has fewer columns
                else
                    "INSERT INTO $catalog.$schemaName.$dstTableName" to ";"
            }
        }

        val withCTE = "WITH CTE AS ( " +
            "SELECT $emittedAt, from_json( ${JavaBaseConstants.COLUMN_NAME_DATA}, \"$srcSchemaString\" ) as data " +
            "FROM $catalog.$schemaName.$srcTableName )"

        val selectQuery = allColumns.joinToString(", ") { dstCol ->
            when {
                srcSchema.containsKey(dstCol) -> "data.`$dstCol`"
                else -> "CAST(NULL AS ${dstSchema[dstCol]}) as $dstCol"
            }
        }.run {
            when {
                isEmpty() -> "$withCTE SELECT $emittedAt FROM CTE ;"
                else -> "$withCTE SELECT $emittedAt, $this FROM CTE $additionalSelect"
            }
        }

        return "$queryAction $selectQuery"
    }

    fun describeTableSchema(
        database: JdbcDatabase,
        catalog: String,
        schemaName: String,
        tableName: String
    ): Map<String, String> = linkedMapOf<String, String>().apply {
        "DESCRIBE $catalog.$schemaName.$tableName".run {
            database.bufferedResultSetQuery(
                { connection -> connection.createStatement().executeQuery(this) },
                { it.getString("col_name").replace("`", "") to it.getString("data_type") },
            ).forEach { put(it.first, it.second) }
        }
    }

    fun mergeSchemas(schemaStrings: List<String>): JsonNode {
        val mapper = ObjectMapper().apply {
            configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
        }
        val mergedSchema = mapper.createObjectNode()

        schemaStrings.forEach { schemaStr ->
            val parsedSchema = parseSchema(schemaStr)
            parsedSchema.forEach { (fieldName, fieldType) ->
                if (!mergedSchema.has(fieldName)) {
                    mergedSchema.put(fieldName, fieldType)
                }
            }
        }

        return mapper.valueToTree(mergedSchema)
    }

    fun jsonToSchemaString(schemaJson: JsonNode): String =
        schemaJson.fields().asSequence().map { (name, type) ->
            "`$name`: ${type.asText()}"
        }.joinToString(", ", prefix = "STRUCT<", postfix = ">")

    fun parseSchema(schema: String): Map<String, String> {
        // Clean up the schema string by removing the 'STRUCT<' prefix and trailing '>', and removing all spaces.
        val schemaStr = schema.removeSurrounding("STRUCT<", ">").replace("\\s+".toRegex(), "")
        val fields = linkedMapOf<String, String>()

        var fieldNameStartIndex = 0
        var currentIndex = 0
        while (currentIndex < schemaStr.length) {
            when (schemaStr[currentIndex]) {
                // On encountering ':', extract the field name and parse its type.
                ':' -> {
                    val fieldName =
                        schemaStr.substring(fieldNameStartIndex, currentIndex).replace("`", "")
                    val (typeName, nextIndex) = parseTypeName(schemaStr, currentIndex + 1)
                    fields[fieldName] = typeName
                    currentIndex = nextIndex + 1
                    fieldNameStartIndex = currentIndex
                }

                else -> currentIndex++
            }
        }

        return fields
    }

    private fun parseTypeName(schemaStr: String, startIndex: Int): Pair<String, Int> {
        val typeBoundsStack = java.util.ArrayDeque<Int>()
        var currentIndex = startIndex
        while (currentIndex < schemaStr.length) {
            when (schemaStr[currentIndex]) {
                '<' -> typeBoundsStack.push(currentIndex)
                '>' -> {
                    typeBoundsStack.pop()
                    // If stack is empty, we've found the end of the current type.
                    if (typeBoundsStack.isEmpty()) {
                        return schemaStr.substring(startIndex, currentIndex + 1) to currentIndex + 1
                    }
                }

                ',' -> if (typeBoundsStack.isEmpty()) {
                    // End of type found without encountering nested types.
                    return schemaStr.substring(startIndex, currentIndex) to currentIndex
                }
            }
            currentIndex++
        }

        // Handles cases without nested types or at the end of the schema.
        return schemaStr.substring(startIndex, currentIndex) to currentIndex
    }
}
