package io.github.noodlemind.sqlkraft.core

sealed class SqlNode

data class SelectNode(
    val columns: List<ColumnNode>, val from: TableNode, val where: WhereNode? = null, val limit: LimitNode? = null
) : SqlNode()

data class CreateTableNode(
    val name: String, val columns: List<ColumnDefinitionNode>
) : SqlNode()

data class ColumnNode(val name: String, val alias: String? = null) : SqlNode()
data class TableNode(val name: String, val alias: String? = null) : SqlNode()
data class WhereNode(val condition: String) : SqlNode()
data class LimitNode(val value: Long?, val offset: Long? = null) : SqlNode()
data class ColumnDefinitionNode(val name: String, val dataType: DataTypeNode) : SqlNode()
data class DataTypeNode(val name: String, val parameters: List<String> = emptyList()) : SqlNode()