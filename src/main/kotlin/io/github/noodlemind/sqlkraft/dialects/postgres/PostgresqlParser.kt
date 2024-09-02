package io.github.noodlemind.sqlkraft.dialects.postgres

import io.github.noodlemind.sqlkraft.core.*
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.statement.select.Select

class PostgresqlParser : SqlParser {
    override fun parse(sql: String): SqlNode {
        val statement = CCJSqlParserUtil.parse(sql)
        return when (statement) {
            is Select -> parseSelect(statement)
            is CreateTable -> parseCreateTable(statement)
            else -> throw UnsupportedOperationException("Unsupported SQL statement type")
        }
    }

    private fun parseSelect(select: Select): SelectNode {
        val selectBody = select.selectBody
        if (selectBody !is PlainSelect) {
            throw UnsupportedOperationException("Only PlainSelect is supported at the moment")
        }

        return SelectNode(
            columns = selectBody.selectItems.map { ColumnNode(it.toString()) },
            from = TableNode(selectBody.fromItem.toString()),
            where = selectBody.where?.let { WhereNode(it.toString()) },
            limit = selectBody.limit?.let {
                LimitNode(
                    value = (it.rowCount as? Long) ?: it.rowCount.toString().toLongOrNull(),
                    offset = it.offset?.let { offset ->
                        (offset as? Long) ?: offset.toString().toLongOrNull()
                    }
                )
            }
        )
    }

    private fun parseCreateTable(createTable: CreateTable): CreateTableNode {
        return CreateTableNode(
            name = createTable.table.name,
            columns = createTable.columnDefinitions.map {
                ColumnDefinitionNode(
                    name = it.columnName,
                    dataType = parseDataType(it.colDataType.dataType, it.colDataType.argumentsStringList)
                )
            }
        )
    }

    private fun parseDataType(dataType: String, parameters: List<String>?): DataTypeNode {
        return DataTypeNode(
            name = when (dataType.lowercase()) {
                "int", "integer" -> "INTEGER"
                "bigint" -> "BIGINT"
                "smallint" -> "SMALLINT"
                "decimal", "numeric" -> "NUMERIC"
                "real" -> "REAL"
                "double precision" -> "DOUBLE PRECISION"
                "serial" -> "SERIAL"
                "bigserial" -> "BIGSERIAL"
                "varchar", "character varying" -> "VARCHAR"
                "char", "character" -> "CHAR"
                "text" -> "TEXT"
                "boolean" -> "BOOLEAN"
                "date" -> "DATE"
                "time" -> "TIME"
                "timestamp" -> "TIMESTAMP"
                "interval" -> "INTERVAL"
                "bytea" -> "BYTEA"
                else -> dataType
            },
            parameters = parameters ?: emptyList()
        )
    }
}
