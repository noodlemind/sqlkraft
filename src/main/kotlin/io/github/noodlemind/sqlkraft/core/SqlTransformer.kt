package io.github.noodlemind.sqlkraft.core

import io.github.noodlemind.sqlkraft.dialects.postgres.PostgresqlParser
import io.github.noodlemind.sqlkraft.dialects.postgres.PostgresqlGenerator
import io.github.noodlemind.sqlkraft.dialects.redshift.RedshiftParser
import io.github.noodlemind.sqlkraft.dialects.redshift.RedshiftGenerator
import kotlinx.coroutines.flow.Flow

class SqlTransformer {
    private val parsers: Map<SqlDialect, SqlParser> = mapOf(
        SqlDialect.POSTGRESQL to PostgresqlParser(),
        SqlDialect.REDSHIFT to RedshiftParser()
    )

    private val generators: Map<SqlDialect, SqlGenerator> = mapOf(
        SqlDialect.POSTGRESQL to PostgresqlGenerator(),
        SqlDialect.REDSHIFT to RedshiftGenerator()
    )

    private val converter: SqlConverter = SqlConverter(parsers, generators)

    fun convert(sql: String, sourceDialect: SqlDialect, targetDialect: SqlDialect): Flow<String> {
        return converter.convert(sql, sourceDialect, targetDialect)
    }
}
