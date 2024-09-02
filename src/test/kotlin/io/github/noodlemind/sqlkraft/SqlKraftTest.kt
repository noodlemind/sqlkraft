package io.github.noodlemind.sqlkraft

import io.github.noodlemind.sqlkraft.api.SqlKraft
import io.github.noodlemind.sqlkraft.core.SqlDialect
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class SqlKraftTest {

    @Test
    fun testPostgresToRedshiftCreateTableConversion() = runBlocking {
        val postgresSQL = "CREATE TABLE users (id SERIAL, name TEXT, age INTEGER, created_at TIMESTAMP);"
        val expectedRedshiftSQL =
            "CREATE TABLE users (id INTEGER IDENTITY(1,1), name VARCHAR(MAX), age INTEGER, created_at TIMESTAMP)"

        val result = SqlKraft.convert(postgresSQL, SqlDialect.POSTGRESQL, SqlDialect.REDSHIFT).first()
        assertEquals(expectedRedshiftSQL, result)
    }

    @Test
    fun testRedshiftToPostgresCreateTableConversion() = runBlocking {
        val redshiftSQL =
            "CREATE TABLE users (id INTEGER IDENTITY(1,1), name VARCHAR(MAX), age INTEGER, created_at TIMESTAMP);"
        val expectedPostgresSQL = "CREATE TABLE users (id SERIAL, name TEXT, age INTEGER, created_at TIMESTAMP)"

        val result = SqlKraft.convert(redshiftSQL, SqlDialect.REDSHIFT, SqlDialect.POSTGRESQL).first()
        assertEquals(expectedPostgresSQL, result)
    }

    @Test
    fun testPostgresToRedshiftSelectConversion() = runBlocking {
        val postgresSQL = "SELECT name, age FROM users WHERE age > 18 LIMIT 10 OFFSET 5;"
        val expectedRedshiftSQL = "SELECT name, age FROM users WHERE age > 18 LIMIT 10"

        val result = SqlKraft.convert(postgresSQL, SqlDialect.POSTGRESQL, SqlDialect.REDSHIFT).first()
        assertEquals(expectedRedshiftSQL, result)
    }

    @Test
    fun testRedshiftToPostgresSelectConversion() = runBlocking {
        val redshiftSQL = "SELECT name, age FROM users WHERE age > 18 LIMIT 10;"
        val expectedPostgresSQL = "SELECT name, age FROM users WHERE age > 18 LIMIT 10 OFFSET 0"

        val result = SqlKraft.convert(redshiftSQL, SqlDialect.REDSHIFT, SqlDialect.POSTGRESQL).first()
        assertEquals(expectedPostgresSQL, result)
    }

    @Test
    fun testPostgresToRedshiftDataTypeConversion() = runBlocking {
        val postgresSQL =
            "CREATE TABLE products (id BIGSERIAL, name VARCHAR(100), price NUMERIC(10,2), description TEXT);"
        val expectedRedshiftSQL =
            "CREATE TABLE products (id BIGINT IDENTITY(1,1), name VARCHAR(100), price NUMERIC(10,2), description VARCHAR(MAX))"

        val result = SqlKraft.convert(postgresSQL, SqlDialect.POSTGRESQL, SqlDialect.REDSHIFT).first()
        assertEquals(expectedRedshiftSQL, result)
    }

    @Test
    fun testRedshiftToPostgresDataTypeConversion() = runBlocking {
        val redshiftSQL =
            "CREATE TABLE orders (id INTEGER IDENTITY(1,1), user_id INTEGER, total DECIMAL(12,2), created_at TIMESTAMPTZ);"
        val expectedPostgresSQL =
            "CREATE TABLE orders (id SERIAL, user_id INTEGER, total NUMERIC(12,2), created_at TIMESTAMP WITH TIME ZONE)"

        val result = SqlKraft.convert(redshiftSQL, SqlDialect.REDSHIFT, SqlDialect.POSTGRESQL).first()
        assertEquals(expectedPostgresSQL, result)
    }

    @Test
    fun testPostgresToRedshiftComplexSelectConversion() = runBlocking {
        val postgresSQL = """
            SELECT u.name, COUNT(o.id) as order_count
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.created_at > '2023-01-01'
            GROUP BY u.name
            HAVING COUNT(o.id) > 5
            ORDER BY order_count DESC
            LIMIT 10 OFFSET 20;
        """.trimIndent()
        val expectedRedshiftSQL = """
            SELECT u.name, COUNT(o.id) as order_count
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.created_at > '2023-01-01'
            GROUP BY u.name
            HAVING COUNT(o.id) > 5
            ORDER BY order_count DESC
            LIMIT 10
        """.trimIndent()

        val result = SqlKraft.convert(postgresSQL, SqlDialect.POSTGRESQL, SqlDialect.REDSHIFT).first()
        assertEquals(expectedRedshiftSQL, result)
    }

    @Test
    fun testRedshiftToPostgresComplexSelectConversion() = runBlocking {
        val redshiftSQL = """
            SELECT p.name, SUM(o.quantity * p.price) as total_revenue
            FROM products p
            INNER JOIN order_items o ON p.id = o.product_id
            WHERE p.category IN ('Electronics', 'Books')
            GROUP BY p.name
            HAVING SUM(o.quantity * p.price) > 1000
            ORDER BY total_revenue DESC
            LIMIT 5;
        """.trimIndent()
        val expectedPostgresSQL = """
            SELECT p.name, SUM(o.quantity * p.price) as total_revenue
            FROM products p
            INNER JOIN order_items o ON p.id = o.product_id
            WHERE p.category IN ('Electronics', 'Books')
            GROUP BY p.name
            HAVING SUM(o.quantity * p.price) > 1000
            ORDER BY total_revenue DESC
            LIMIT 5 OFFSET 0
        """.trimIndent()

        val result = SqlKraft.convert(redshiftSQL, SqlDialect.REDSHIFT, SqlDialect.POSTGRESQL).first()
        assertEquals(expectedPostgresSQL, result)
    }
}
