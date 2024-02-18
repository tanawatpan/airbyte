package io.airbyte.integrations.destination.databricks

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.cdk.db.jdbc.JdbcDatabase
import io.airbyte.commons.functional.CheckedFunction
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.mockk.*
import org.jooq.Record1
import org.jooq.Result
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException

typealias ResultRecord = Result<Record1<String>>

class DatabricksSqlOperationsTest {

    private val sqlOperations = spyk(DatabricksSqlOperations())
    private val mapper = ObjectMapper()

    private val schemaStrings = listOf(
        "STRUCT<field1: STRING, field2: INT, field3: STRUCT<subfield1:STRING,subfield2:INT>>",
        "STRUCT<field4: ARRAY<STRING>, field5: BOOLEAN>",
        "STRUCT<field6: MAP<STRING,DOUBLE>>"
    )
    private val schemaJson = mapper.createObjectNode().apply {
        put("field1", "STRING")
        put("field2", "INT")
        put("field3", "STRUCT<subfield1:STRING,subfield2:INT>")
        put("field4", "ARRAY<STRING>")
        put("field5", "BOOLEAN")
        put("field6", "MAP<STRING,DOUBLE>")
    }

    @Test
    fun `test jsonToSchemaString`() {
        val expected =
            "STRUCT<`field1`: STRING, `field2`: INT, `field3`: STRUCT<subfield1:STRING,subfield2:INT>, `field4`: ARRAY<STRING>, `field5`: BOOLEAN, `field6`: MAP<STRING,DOUBLE>>"

        val result = sqlOperations.jsonToSchemaString(schemaJson)

        assertEquals(expected, result)
    }

    @Test
    fun `test mergeSchemas`() {
        val result = sqlOperations.mergeSchemas(schemaStrings)
        assertEquals(schemaJson, result)
    }

    @Test
    fun `test getTableSchemaString`() {
        val catalog = "catalog"
        val schemaName = "schemaName"
        val tmpTableName = "tmpTableName"

        val database = mockk<JdbcDatabase>()
        every {
            database.bufferedResultSetQuery(
                any<CheckedFunction<Connection, ResultSet, SQLException>>(),
                any<CheckedFunction<ResultSet, String, SQLException>>()
            )
        } returns schemaStrings

        val expected =
            "STRUCT<`field1`: STRING, `field2`: INT, `field3`: STRUCT<subfield1:STRING,subfield2:INT>, `field4`: ARRAY<STRING>, `field5`: BOOLEAN, `field6`: MAP<STRING,DOUBLE>>"

        val actual = sqlOperations.getTableSchemaString(database, catalog, schemaName, tmpTableName)
        assertEquals(expected, actual)

        verify(exactly = 1) {
            sqlOperations.getTableSchemaString(database, catalog, schemaName, tmpTableName)
            sqlOperations.mergeSchemas(schemaStrings)
            sqlOperations.jsonToSchemaString(schemaJson)
            schemaStrings.forEach { sqlOperations.parseSchema(it) }
        }

        confirmVerified(sqlOperations)
    }

    private val catalog = "catalog"
    private val schemaName = "schemaName"
    private val srcTableName = "srcTable"
    private val dstTableName = "dstTable"

    @Test
    fun `finalizeTableQuery should handle scenario when destination has fewer columns`() {
        val srcSchema = mapOf("id" to "int", "name" to "string", "age" to "int")
        val dstSchema = mapOf("id" to "int", "name" to "string")
        val syncMode = DestinationSyncMode.APPEND
        val expectedSQL =
            "CREATE OR REPLACE TABLE $catalog.$schemaName.$dstTableName AS WITH CTE AS ( SELECT _airbyte_emitted_at, from_json( _airbyte_data, \"STRUCT<`id`:int,`name`:string,`age`:int>\" ) as data FROM $catalog.$schemaName.$srcTableName ) SELECT _airbyte_emitted_at, data.`id`, data.`name`, data.`age` FROM CTE UNION ALL SELECT _airbyte_emitted_at, id, name, CAST(NULL AS int) as age FROM $catalog.$schemaName.$dstTableName ;"

        val actualSQL =
            sqlOperations.finalizeTableQuery(
                syncMode,
                srcSchema,
                dstSchema,
                catalog,
                schemaName,
                srcTableName,
                dstTableName
            )

        assertEquals(expectedSQL, actualSQL)
    }

    @Test
    fun `finalizeTableQuery should handle scenario when schemas match`() {
        val srcSchema = mapOf("id" to "int", "name" to "string")
        val dstSchema = mapOf("id" to "int", "name" to "string")
        val syncMode = DestinationSyncMode.APPEND
        val expectedSQL =
            "INSERT INTO $catalog.$schemaName.$dstTableName WITH CTE AS ( SELECT _airbyte_emitted_at, from_json( _airbyte_data, \"STRUCT<`id`:int,`name`:string>\" ) as data FROM $catalog.$schemaName.$srcTableName ) SELECT _airbyte_emitted_at, data.`id`, data.`name` FROM CTE ;"

        val actualSQL =
            sqlOperations.finalizeTableQuery(
                syncMode,
                srcSchema,
                dstSchema,
                catalog,
                schemaName,
                srcTableName,
                dstTableName
            )

        assertEquals(expectedSQL, actualSQL)
    }

    @Test
    fun `finalizeTableQuery should handle scenario when source has fewer columns`() {
        val srcSchema = mapOf("id" to "int", "name" to "string")
        val dstSchema = mapOf("id" to "int", "name" to "string", "age" to "int")
        val syncMode = DestinationSyncMode.APPEND
        val expectedSQL =
            "INSERT INTO $catalog.$schemaName.$dstTableName WITH CTE AS ( SELECT _airbyte_emitted_at, from_json( _airbyte_data, \"STRUCT<`id`:int,`name`:string>\" ) as data FROM $catalog.$schemaName.$srcTableName ) SELECT _airbyte_emitted_at, data.`id`, data.`name`, CAST(NULL AS int) as age FROM CTE ;"

        val actualSQL =
            sqlOperations.finalizeTableQuery(
                syncMode,
                srcSchema,
                dstSchema,
                catalog,
                schemaName,
                srcTableName,
                dstTableName
            )

        assertEquals(expectedSQL, actualSQL)
    }

    @Test
    fun `finalizeTableQuery should handle scenario when OVERWRITE mode is selected`() {
        val srcSchema = mapOf("id" to "int", "name" to "string")
        val dstSchema = mapOf("id" to "int", "name" to "string")
        val syncMode = DestinationSyncMode.OVERWRITE
        val expectedSQL =
            "CREATE OR REPLACE TABLE $catalog.$schemaName.$dstTableName AS WITH CTE AS ( SELECT _airbyte_emitted_at, from_json( _airbyte_data, \"STRUCT<`id`:int,`name`:string>\" ) as data FROM $catalog.$schemaName.$srcTableName ) SELECT _airbyte_emitted_at, data.`id`, data.`name` FROM CTE ;"

        val actualSQL =
            sqlOperations.finalizeTableQuery(
                syncMode,
                srcSchema,
                dstSchema,
                catalog,
                schemaName,
                srcTableName,
                dstTableName
            )

        assertEquals(expectedSQL, actualSQL)
    }

    @Test
    fun `finalizeTableQuery should handle scenario when APPEND mode is selected and destination has fewer columns`() {
        val srcSchema = mapOf("id" to "int", "name" to "string", "age" to "int")
        val dstSchema = mapOf("id" to "int", "name" to "string")
        val syncMode = DestinationSyncMode.APPEND
        val expectedSQL =
            "CREATE OR REPLACE TABLE $catalog.$schemaName.$dstTableName AS WITH CTE AS ( SELECT _airbyte_emitted_at, from_json( _airbyte_data, \"STRUCT<`id`:int,`name`:string,`age`:int>\" ) as data FROM $catalog.$schemaName.$srcTableName ) SELECT _airbyte_emitted_at, data.`id`, data.`name`, data.`age` FROM CTE UNION ALL SELECT _airbyte_emitted_at, id, name, CAST(NULL AS int) as age FROM $catalog.$schemaName.$dstTableName ;"

        val actualSQL =
            sqlOperations.finalizeTableQuery(
                syncMode,
                srcSchema,
                dstSchema,
                catalog,
                schemaName,
                srcTableName,
                dstTableName
            )

        assertEquals(expectedSQL, actualSQL)
    }
}