package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;

import org.tarantool.ServerVersion;
import org.tarantool.TarantoolTestHelper;
import org.tarantool.TestUtils;
import org.tarantool.util.SQLStates;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

public class JdbcPreparedStatementIT {

    private static final String[] INIT_SQL = new String[] {
        "CREATE TABLE test(id INT PRIMARY KEY, val VARCHAR(100))",
    };

    private static final String[] CLEAN_SQL = new String[] {
        "DROP TABLE IF EXISTS test"
    };

    private static TarantoolTestHelper testHelper;
    private static Connection conn;

    private PreparedStatement prep;

    @BeforeAll
    public static void setupEnv() throws SQLException {
        testHelper = new TarantoolTestHelper("jdbc-prepared-it");
        testHelper.createInstance();
        testHelper.startInstance();

        conn = DriverManager.getConnection(SqlTestUtils.makeDefaultJdbcUrl());
    }

    @AfterAll
    public static void teardownEnv() throws SQLException {
        if (conn != null) {
            conn.close();
        }
        testHelper.stopInstance();
    }

    @BeforeEach
    public void setUpTest() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        testHelper.executeSql(INIT_SQL);
    }

    @AfterEach
    public void tearDownTest() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        testHelper.executeSql(CLEAN_SQL);

        if (prep != null) {
            prep.close();
        }
    }

    @Test
    public void testExecuteQuery() throws SQLException {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one'), (2, 'two')");

        prep = conn.prepareStatement("SELECT val FROM test WHERE id = ?");
        assertNotNull(prep);

        prep.setInt(1, 1);
        ResultSet rs = prep.executeQuery();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("one", rs.getString(1));
        assertFalse(rs.next());
        rs.close();

        // Reuse the prepared statement.
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("two", rs.getString(1));
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testExecuteWrongQuery() throws SQLException {
        prep = conn.prepareStatement("INSERT INTO test VALUES (?, ?)");
        prep.setInt(1, 200);
        prep.setString(2, "two hundred");

        SQLException exception = assertThrows(SQLException.class, () -> prep.executeQuery());
        SqlAssertions.assertSqlExceptionHasStatus(exception, SQLStates.NO_DATA);
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test VALUES(?, ?)");
        assertNotNull(prep);

        prep.setInt(1, 100);
        prep.setString(2, "hundred");
        int count = prep.executeUpdate();
        assertEquals(1, count);

        assertEquals("hundred", consoleSelect(100).get(1));

        // Reuse the prepared statement.
        prep.setInt(1, 1000);
        prep.setString(2, "thousand");
        count = prep.executeUpdate();
        assertEquals(1, count);

        assertEquals("thousand", consoleSelect(1000).get(1));
    }

    @Test
    public void testExecuteWrongUpdate() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test WHERE id=?");
        prep.setInt(1, 1);

        SQLException exception = assertThrows(SQLException.class, () -> prep.executeUpdate());
        SqlAssertions.assertSqlExceptionHasStatus(exception, SQLStates.TOO_MANY_RESULTS);
    }

    @Test
    public void testExecuteReturnsResultSet() throws SQLException {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one')");

        prep = conn.prepareStatement("SELECT val FROM test WHERE id=?");
        prep.setInt(1, 1);

        assertTrue(prep.execute());
        assertEquals(-1, prep.getUpdateCount());

        try (ResultSet resultSet = prep.getResultSet()) {
            assertNotNull(resultSet);
            assertTrue(resultSet.next());
            assertEquals("one", resultSet.getString(1));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testExecuteReturnsUpdateCount() throws Exception {
        prep = conn.prepareStatement("INSERT INTO test VALUES(?, ?), (?, ?)");
        assertNotNull(prep);

        prep.setInt(1, 10);
        prep.setString(2, "ten");
        prep.setInt(3, 20);
        prep.setString(4, "twenty");

        assertFalse(prep.execute());
        assertNull(prep.getResultSet());
        assertEquals(2, prep.getUpdateCount());

        assertEquals("ten", consoleSelect(10).get(1));
        assertEquals("twenty", consoleSelect(20).get(1));
    }

    @Test
    void testForbiddenMethods() throws SQLException {
        prep = conn.prepareStatement("TEST");

        int i = 0;
        for (; i < 3; i++) {
            final int step = i;
            SQLException e = assertThrows(SQLException.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    switch (step) {
                    case 0:
                        prep.executeQuery("TEST");
                        break;
                    case 1:
                        prep.executeUpdate("TEST");
                        break;
                    case 2:
                        prep.execute("TEST");
                        break;
                    default:
                        fail();
                    }
                }
            });
            assertEquals("The method cannot be called on a PreparedStatement.", e.getMessage());
        }
        assertEquals(3, i);
    }

    @Test
    public void testUnwrap() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test");
        assertEquals(prep, prep.unwrap(SQLPreparedStatement.class));
        assertEquals(prep, prep.unwrap(SQLStatement.class));
        assertThrows(SQLException.class, () -> prep.unwrap(Integer.class));
    }

    @Test
    public void testIsWrapperFor() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test");
        assertTrue(prep.isWrapperFor(SQLPreparedStatement.class));
        assertTrue(prep.isWrapperFor(SQLStatement.class));
        assertFalse(prep.isWrapperFor(Integer.class));
    }

    @Test
    public void testSupportGeneratedKeys() throws SQLException {
        prep = conn.prepareStatement("INSERT INTO test values (50, 'fifty')", Statement.NO_GENERATED_KEYS);
        assertFalse(prep.execute());
        assertEquals(1, prep.getUpdateCount());

        ResultSet generatedKeys = prep.getGeneratedKeys();
        assertNotNull(generatedKeys);
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, generatedKeys.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, generatedKeys.getConcurrency());
    }

    @Test
    void testStatementConnection() throws SQLException {
        Statement statement = conn.prepareStatement("SELECT * FROM TEST");
        assertEquals(conn, statement.getConnection());
    }

    @Test
    public void testMoreResultsWithResultSet() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test WHERE id = ?");
        prep.setInt(1, 1);

        prep.execute();
        ResultSet resultSet = prep.getResultSet();

        assertFalse(resultSet.isClosed());
        assertFalse(prep.getMoreResults());
        assertEquals(-1, prep.getUpdateCount());
        assertTrue(resultSet.isClosed());
    }

    @Test
    public void testMoreResultsWithUpdateCount() throws SQLException {
        prep = conn.prepareStatement("INSERT INTO test VALUES (?, ?)");
        prep.setInt(1, 9);
        prep.setString(2, "nine");

        prep.execute();
        int updateCount = prep.getUpdateCount();

        assertEquals(1, prep.getUpdateCount());
        assertFalse(prep.getMoreResults());
        assertEquals(-1, prep.getUpdateCount());
    }

    @Test
    public void testMoreResultsButCloseCurrent() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test WHERE id = ?");
        prep.setInt(1, 2);

        prep.execute();
        ResultSet resultSet = prep.getResultSet();

        assertFalse(resultSet.isClosed());
        assertFalse(prep.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
        assertEquals(-1, prep.getUpdateCount());
        assertTrue(resultSet.isClosed());
    }

    @Test
    public void testMoreResultsButCloseAll() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test WHERE id = ?");
        prep.setInt(1, 2);
        prep.execute();

        assertThrows(SQLFeatureNotSupportedException.class, () -> prep.getMoreResults(Statement.CLOSE_ALL_RESULTS));

        prep = conn.prepareStatement("INSERT INTO test VALUES (?, ?)");
        prep.setInt(1, 21);
        prep.setString(2, "twenty one");
        prep.execute();

        assertEquals(1, prep.getUpdateCount());
        assertFalse(prep.getMoreResults(Statement.CLOSE_ALL_RESULTS));
        assertEquals(-1, prep.getUpdateCount());
    }

    @Test
    public void testMoreResultsButKeepCurrent() throws SQLException {
        prep = conn.prepareStatement("SELECT val FROM test WHERE id = ?");
        prep.setInt(1, 3);
        prep.execute();

        assertThrows(SQLFeatureNotSupportedException.class, () -> prep.getMoreResults(Statement.KEEP_CURRENT_RESULT));

        prep = conn.prepareStatement("INSERT INTO test VALUES (?, ?)");
        prep.setInt(1, 22);
        prep.setString(2, "twenty two");
        prep.execute();

        assertEquals(1, prep.getUpdateCount());
        assertFalse(prep.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        assertEquals(-1, prep.getUpdateCount());
    }

    @Test
    public void testDisabledEscapeSyntax() throws Exception {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')");

        prep = conn.prepareStatement("SELECT val FROM test ORDER BY id {limit ?}");
        prep.setEscapeProcessing(false);
        prep.setInt(1, 1);

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals("one", resultSet.getString(1));
            assertFalse(resultSet.next());
        }

    }

    @Test
    public void testLimitEscapeSyntax() throws Exception {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')");

        prep = conn.prepareStatement("SELECT val FROM test ORDER BY id {limit ? offset ?}");
        prep.setInt(1, 2);
        prep.setInt(2, 0);

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals("one", resultSet.getString(1));
            assertTrue(resultSet.next());
            assertEquals("two", resultSet.getString(1));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testLikeEscapeSyntax() throws Exception {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one%'), (2, 'two'), (3, 'three%'), (4, 'four')");

        prep = conn.prepareStatement("SELECT val FROM test WHERE val LIKE '%|%' {escape ?}");
        prep.setString(1, "|");

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals("one%", resultSet.getString(1));
            assertTrue(resultSet.next());
            assertEquals("three%", resultSet.getString(1));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testOuterJoinEscapeSyntax() throws Exception {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one')");

        prep = conn.prepareStatement(
            "SELECT {fn concat('t1-', t1.val)}, {fn concat('t2-', t2.val)} " +
                "FROM {oj test t1 LEFT OUTER JOIN test t2 ON t1.id = ?}"
        );
        prep.setInt(1, 1);

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals("t1-one", resultSet.getString(1));
            assertEquals("t2-one", resultSet.getString(2));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testSystemFunctionEscapeSyntax() throws Exception {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, NULL)");

        prep = conn.prepareStatement("SELECT {fn ifnull(val, ?)} FROM test WHERE id = 1");
        prep.setString(1, "one-one");

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals("one-one", resultSet.getString(1));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testNumericFunctionEscapeSyntax() throws Exception {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, NULL)");

        prep = conn.prepareStatement("SELECT {fn abs(5 - ?)}, {fn round({fn pi()}, ?)}");
        prep.setInt(1, 10);
        prep.setInt(2, 0);

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals(5, resultSet.getInt(1));
            assertEquals(3, resultSet.getInt(2));
            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testStringFunctionEscapeSyntax() throws Exception {
        testHelper.executeSql("INSERT INTO test(id, val) VALUES (1, 'one'), (2, 'TWO'), (3, 'three'), (4, ' four ')");

        prep = conn.prepareStatement(
            "SELECT {fn char(?)}, {fn char_length(val)}, {fn concat(?, val)} FROM test WHERE id = 3"
        );
        prep.setInt(1, 0x20);
        prep.setString(2, "3 ");

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals(" ", resultSet.getString(1));
            assertEquals(5, resultSet.getInt(2));
            assertEquals("3 three", resultSet.getString(3));
            assertFalse(resultSet.next());
        }
        prep.close();

        prep = conn.prepareStatement(
            "SELECT {fn lcase(val)}, " +
                "{fn left(val, ?)}, " +
                "{fn replace({fn lcase(val)}, 'two', ?)}, " +
                "{fn substring(val, ?, 2)} " +
                "FROM test WHERE id = 2"
        );
        prep.setInt(1, 2);
        prep.setString(2, "2");
        prep.setInt(3, 1);

        prep.execute();

        try (ResultSet resultSet = prep.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals("two", resultSet.getString(1));
            assertEquals("TW", resultSet.getString(2));
            assertEquals("2", resultSet.getString(3));
            assertEquals("TW", resultSet.getString(4));
            assertFalse(resultSet.next());
        }
    }

    private List<?> consoleSelect(Object key) {
        List<?> list = testHelper.evaluate(TestUtils.toLuaSelect("TEST", key));
        return list == null ? Collections.emptyList() : (List<?>) list.get(0);
    }

}
