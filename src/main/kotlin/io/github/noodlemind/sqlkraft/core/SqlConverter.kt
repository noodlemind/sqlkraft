package io.github.noodlemind.sqlkraft.core

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

class SqlConverter(
    private val parsers: Map<SqlDialect, SqlParser>, private val generators: Map<SqlDialect, SqlGenerator>
) {
    fun convert(sql: String, sourceDialect: SqlDialect, targetDialect: SqlDialect): Flow<String> = flow {
        val sourceParser =
            parsers[sourceDialect] ?: throw UnsupportedOperationException("Unsupported source dialect: $sourceDialect")
        val targetGenerator = generators[targetDialect]
            ?: throw UnsupportedOperationException("Unsupported target dialect: $targetDialect")

        val ast = sourceParser.parse(sql)
        val convertedSql = targetGenerator.generate(ast)
        emit(convertedSql)
    }
}
