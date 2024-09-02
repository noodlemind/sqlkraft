package io.github.noodlemind.sqlkraft.dialects.postgres

import io.github.noodlemind.sqlkraft.core.*

class PostgresqlGenerator : SqlGenerator {
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
        val limit = node.limit?.let {
            "LIMIT ${it.value ?: "ALL"}" + (it.offset?.let { offset -> " OFFSET $offset" } ?: "")
        } ?: ""
        return "SELECT $columns $from $where $limit".trim()
    }

    private fun generateCreateTable(node: CreateTableNode): String {
        val columns = node.columns.joinToString(", ") { "${it.name} ${generateDataType(it.dataType)}" }
        return "CREATE TABLE ${node.name} ($columns)"
    }

    private fun generateDataType(dataType: DataTypeNode): String {
        val typeName = when (dataType.name.uppercase()) {
            "INTEGER" -> "INT"
            "BIGINT" -> "BIGINT"
            "SMALLINT" -> "SMALLINT"
            "NUMERIC" -> "NUMERIC"
            "REAL" -> "REAL"
            "DOUBLE PRECISION" -> "DOUBLE PRECISION"
            "SERIAL" -> "SERIAL"
            "BIGSERIAL" -> "BIGSERIAL"
            "VARCHAR" -> "VARCHAR"
            "CHAR" -> "CHAR"
            "TEXT" -> "TEXT"
            "BOOLEAN" -> "BOOLEAN"
            "DATE" -> "DATE"
            "TIME" -> "TIME"
            "TIMESTAMP" -> "TIMESTAMP"
            "INTERVAL" -> "INTERVAL"
            "BYTEA" -> "BYTEA"
            else -> dataType.name
        }
        return if (dataType.parameters.isEmpty()) typeName else "$typeName(${dataType.parameters.joinToString(", ")})"
    }
}
