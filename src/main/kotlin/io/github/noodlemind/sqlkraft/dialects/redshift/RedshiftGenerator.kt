package io.github.noodlemind.sqlkraft.dialects.redshift

import io.github.noodlemind.sqlkraft.core.*

class RedshiftGenerator : SqlGenerator {
    override fun generate(node: SqlNode): String {
        return when (node) {
            is SelectNode -> generateSelect(node)
            is CreateTableNode -> generateCreateTable(node)
            else -> throw UnsupportedOperationException("Unsupported SQL node type")
        }
    }

    private fun generateSelect(node: SelectNode): String {
        val columns = node.columns.joinToString(", ") { it.name }
        val from = "FROM ${node.from.name}"
        val where = node.where?.let { "WHERE ${it.condition}" } ?: ""
        val limit = node.limit?.let { "LIMIT ${it.value ?: "ALL"}" } ?: ""
        return "SELECT $columns $from $where $limit".trim()
    }

    private fun generateCreateTable(node: CreateTableNode): String {
        val columns = node.columns.joinToString(", ") { "${it.name} ${generateDataType(it.dataType)}" }
        return "CREATE TABLE ${node.name} ($columns)"
    }

    private fun generateDataType(dataType: DataTypeNode): String {
        val typeName = when (dataType.name.uppercase()) {
            "SERIAL" -> "INTEGER IDENTITY(1,1)"
            "TEXT" -> "VARCHAR(MAX)"
            "SMALLINT" -> "SMALLINT"
            "INTEGER" -> "INTEGER"
            "BIGINT" -> "BIGINT"
            "NUMERIC" -> "NUMERIC"
            "REAL" -> "REAL"
            "DOUBLE PRECISION" -> "DOUBLE PRECISION"
            "BOOLEAN" -> "BOOLEAN"
            "CHAR" -> "CHAR"
            "VARCHAR" -> "VARCHAR"
            "DATE" -> "DATE"
            "TIMESTAMP" -> "TIMESTAMP"
            "TIMESTAMPTZ" -> "TIMESTAMPTZ"
            "TIME" -> "TIME"
            "TIMETZ" -> "TIMETZ"
            else -> dataType.name
        }
        return if (dataType.parameters.isEmpty()) typeName else "$typeName(${dataType.parameters.joinToString(", ")})"
    }
}
