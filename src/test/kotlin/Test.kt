import org.jooq.impl.DSL.*
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.assertThrows
import java.lang.NullPointerException
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

internal class Test {
    val conn = DriverManager.getConnection("jdbc:tc:mysql:8:///databasename")
    val ctx = using(conn)

    @Test
    fun testMySQLNormalFields() {
        val id = field(name("id"), SQLDataType.INTEGER.notNull())
        val v = field(name("v"), SQLDataType.VARCHAR.notNull())

        val testTable = table(name("test_table_1"))

        ctx.createTable(testTable)
            .column(id)
            .column(v)
            .primaryKey(id)
            .execute()

        val res = ctx.insertInto(testTable)
            .set(mapOf(id to 1, v to "foo"))
            // returningResult exhibits the same behavior, nothing returned
            .returning(id, v)
            .fetchOne()

        // This shouldn't be null though, we are meant to get our row back!
        assertNull(res)

        // The row was properly inserted however
        val row = ctx.selectFrom(testTable).fetchOne()!!.intoMap()
        assertEquals(mapOf("id" to 1, "v" to "foo"), row)
    }

    @Test
    fun testMySQLIdentityWorks() {

        // This time lets use a generated id
        val id = field(name("id"), SQLDataType.INTEGER.identity(true))
        val v = field(name("v"), SQLDataType.VARCHAR.notNull())

        val testTable = table(name("test_table_2"))

        ctx.createTable(testTable)
            .column(id)
            .column(v)
            .primaryKey(id)
            .execute()

        val res = ctx.insertInto(testTable)
            .set(mapOf(id to 1, v to "foo"))
            .returningResult(id)
            .fetchOne()!!.intoMap()

        // Fetching -just- the generated value does work
        assertEquals(mapOf("id" to 1), res)

        val row = ctx.selectFrom(testTable).fetchOne()!!.intoMap()
        assertEquals(mapOf("id" to 1, "v" to "foo"), row)
    }

    @Test
    fun testMySQLIdentityWithOtherRows() {
        val id = field(name("id"), SQLDataType.INTEGER.identity(true))
        val v = field(name("v"), SQLDataType.VARCHAR.notNull())

        val testTable = table(name("test_table_3"))

        ctx.createTable(testTable)
            .column(id)
            .column(v)
            .primaryKey(id)
            .execute()

        // If we try to return the non-identity column it blows up with:
        // java.lang.NullPointerException: Cannot invoke "org.jooq.Field.in(Object[])" because the return value of "org.jooq.Table.field(org.jooq.Field)" is null
        assertThrows<NullPointerException> {
            ctx.insertInto(testTable)
                .set(mapOf(id to 1, v to "foo"))
                .returningResult(id, v)
                .fetchOne()!!.intoMap()
        }
    }
}