package io.github.noodlemind.sqlkraft.core

interface SqlParser {
    fun parse(sql: String): SqlNode
}