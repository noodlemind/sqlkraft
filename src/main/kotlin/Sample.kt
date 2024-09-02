import io.github.noodlemind.sqlkraft.api.SqlKraft
import io.github.noodlemind.sqlkraft.core.SqlDialect
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
fun main() = runBlocking {
    val sqlKraft = SqlKraft.instance
    val sqlStatements = listOf(
        "CREATE TABLE users (id INTEGER, name VARCHAR(255), email VARCHAR(255), created_at TIMESTAMP);",
        "SELECT name, email FROM users WHERE created_at > '2024-01-01' LIMIT 10 OFFSET 20;",
    )
    println("Converting from PostgreSQL to Redshift:")
    sqlStatements.forEach { sql ->
        println("\nOriginal PostgresSQL:")
        println(sql)
        println("Converted Redshift:")
        sqlKraft.convert(sql, SqlDialect.POSTGRESQL, SqlDialect.REDSHIFT).collect { converted ->
            println(converted)
        }
    }
    println("\n\nConverting from Redshift to PostgresSQL:")
    sqlStatements.forEach { sql ->
        println("\nOriginal Redshift:")
        println(sql)
        println("Converted PostgresSQL:")
        sqlKraft.convert(sql, SqlDialect.REDSHIFT, SqlDialect.POSTGRESQL).collect { converted ->
            println(converted)
        }
    }
}