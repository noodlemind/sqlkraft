package io.github.noodlemind.sqlkraft.api

import io.github.noodlemind.sqlkraft.core.*
import kotlinx.coroutines.flow.Flow

class SqlKraft private constructor() {
    private val transformer: SqlTransformer = SqlTransformer()

    fun convert(sql: String, sourceDialect: SqlDialect, targetDialect: SqlDialect): Flow<String> {
        return transformer.convert(sql, sourceDialect, targetDialect)
    }

    companion object {
        val instance: SqlKraft by lazy { SqlKraft() }
    }
}