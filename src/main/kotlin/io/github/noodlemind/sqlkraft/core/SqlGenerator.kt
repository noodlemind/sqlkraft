package io.github.noodlemind.sqlkraft.core

interface SqlGenerator {
    fun generate(sqlNode: SqlNode): String
}