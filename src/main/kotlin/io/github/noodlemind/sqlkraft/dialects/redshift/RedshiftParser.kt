package io.github.noodlemind.sqlkraft.dialects.redshift

import io.github.noodlemind.sqlkraft.core.*
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.parser.feature.Feature.limit
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.statement.select.Select
import java.lang.ScopedValue.where

class RedshiftParser : SqlParser {
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

        return SelectNode(columns = selectBody.selectItems.map { ColumnNode(it.toString()) },
            from = TableNode(selectBody.fromItem.toString()),
            where = selectBody.where?.let { WhereNode(it.toString()) },
            limit = selectBody.limit?.let {
                LimitNode(
                    value = (it.rowCount as? Long) ?: it.rowCount.toString().toLongOrNull(),
                    offset = null
                )
            })
    }

    private fun parseCreateTable(createTable: CreateTable): CreateTableNode {
        return CreateTableNode(name = createTable.table.name, columns = createTable.columnDefinitions.map {
            ColumnDefinitionNode(
                name = it.columnName,
                dataType = parseDataType(it.colDataType.dataType, it.colDataType.argumentsStringList)
            )
        })
    }

    private fun parseDataType(dataType: String, parameters: List<String>?): DataTypeNode {
        return DataTypeNode(
            name = when (dataType.lowercase()) {
                "smallint" -> "SMALLINT"
                "integer", "int", "int4" -> "INTEGER"
                "bigint", "int8" -> "BIGINT"
                "decimal", "numeric" -> "NUMERIC"
                "real", "float4" -> "REAL"
                "double precision", "float8" -> "DOUBLE PRECISION"
                "boolean", "bool" -> "BOOLEAN"
                "char", "character", "nchar", "bpchar" -> "CHAR"
                "varchar", "character varying", "nvarchar" -> "VARCHAR"
                "date" -> "DATE"
                "timestamp", "timestamp without time zone" -> "TIMESTAMP"
                "timestamptz", "timestamp with time zone" -> "TIMESTAMPTZ"
                "time", "time without time zone" -> "TIME"
                "timetz", "time with time zone" -> "TIMETZ"
                else -> dataType
            }, parameters = parameters ?: emptyList()
        )
    }
}
