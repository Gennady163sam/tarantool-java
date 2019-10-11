package org.tarantool;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.tarantool.dsl.Requests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ClientResultSetIT {

    private static TarantoolTestHelper testHelper;

    private TarantoolClient client;

    @BeforeAll
    static void setupEnv() {
        testHelper = new TarantoolTestHelper("client-resultset-it");
        testHelper.createInstance();
        testHelper.startInstance();
    }

    @BeforeEach
    void setUp() {
        client = TestUtils.makeTestClient(TestUtils.makeDefaultClientConfig(), 2000);
    }

    @AfterEach
    void tearDown() {
        client.close();
    }

    @AfterAll
    static void tearDownEnv() {
        testHelper.stopInstance();
    }

    @Test
    void testGetSimpleRows() {
        testHelper.executeLua(
            "box.schema.space.create('basic_test', { format = " +
                "{{name = 'id', type = 'integer'}," +
                "{name = 'num', type = 'integer', is_nullable = true}," +
                "{name = 'val', type = 'string', is_nullable = true} }})",
            "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )",
            "box.space.basic_test:insert{1, nil, 'string'}",
            "box.space.basic_test:insert{2, 50, nil}",
            "box.space.basic_test:insert{3, 123, 'some'}",
            "box.space.basic_test:insert{4, -89, '89'}",
            "box.space.basic_test:insert{5, 93127, 'too many'}",
            "box.space.basic_test:insert{6, nil, nil}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("basic_test", "pk"));
        resultSet.next();
        assertEquals(0, resultSet.getInt(1));
        assertEquals("string", resultSet.getString(2));

        // all ending nil values are trimmed
        resultSet.next();
        assertEquals(50, resultSet.getInt(1));
        assertEquals(2, resultSet.getRowSize());

        resultSet.next();
        assertEquals(123, resultSet.getInt(1));
        assertEquals("some", resultSet.getString(2));

        resultSet.next();
        assertEquals(-89, resultSet.getInt(1));
        assertEquals("89", resultSet.getString(2));

        resultSet.next();
        assertEquals(93127, resultSet.getInt(1));
        assertEquals("too many", resultSet.getString(2));

        // all ending nil values are trimmed
        resultSet.next();
        assertEquals(1, resultSet.getRowSize());

        dropSpace("basic_test");
    }

    @Test
    void testResultTraversal() {
        testHelper.executeLua(
            "box.schema.space.create('basic_test', {format={{name = 'id', type = 'integer'}}})",
            "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )",
            "box.space.basic_test:insert{1}",
            "box.space.basic_test:insert{2}",
            "box.space.basic_test:insert{3}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("basic_test", "pk"));

        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt(0));

        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getInt(0));

        assertTrue(resultSet.next());
        assertEquals(3, resultSet.getInt(0));

        assertFalse(resultSet.next());

        assertTrue(resultSet.previous());
        assertEquals(2, resultSet.getInt(0));

        assertTrue(resultSet.previous());
        assertEquals(1, resultSet.getInt(0));

        assertFalse(resultSet.previous());

        dropSpace("basic_test");
    }

    @Test
    void testResultClose() throws IOException {
        testHelper.executeLua(
            "box.schema.space.create('basic_test', {format={{name = 'id', type = 'integer'}}})",
            "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )",
            "box.space.basic_test:insert{1}",
            "box.space.basic_test:insert{2}",
            "box.space.basic_test:insert{3}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("basic_test", "pk"));

        resultSet.next();
        assertEquals(3, resultSet.size());
        assertEquals(0, resultSet.getRow());
        assertEquals(1, resultSet.getRowSize());

        resultSet.close();
        assertEquals(0, resultSet.size());
        assertEquals(-1, resultSet.getRowSize());
        assertEquals(-1, resultSet.getRow());

        dropSpace("basic_test");
    }

    @Test
    void testGetEmptyResult() {
        testHelper.executeLua(
            "box.schema.space.create('basic_test', {format={{name = 'id', type = 'integer'}}})",
            "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("basic_test", "pk"));

        assertTrue(resultSet.isEmpty());
        assertFalse(resultSet.next());

        dropSpace("basic_test");
    }

    @Test
    void testGetWrongColumn() {
        testHelper.executeLua(
            "box.schema.space.create('basic_test', {format={{name = 'id', type = 'integer'}}})",
            "box.space.basic_test:create_index('pk', { type = 'TREE', parts = {'id'} } )",
            "box.space.basic_test:insert{1}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("basic_test", "pk"));

        resultSet.next();
        assertThrows(TarantoolClientException.class, () -> resultSet.getByte(2));

        dropSpace("basic_test");
    }

    @Test
    void testGetByteValue() {
        testHelper.executeLua(
            "box.schema.space.create('byte_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'byte_val', type = 'integer', is_nullable = true} }" +
                "})",
            "box.space.byte_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.byte_vals:insert{1, -128}",
            "box.space.byte_vals:insert{2, 127}",
            "box.space.byte_vals:insert{3, nil}",
            "box.space.byte_vals:insert{4, 0}",
            "box.space.byte_vals:insert{5, 15}",
            "box.space.byte_vals:insert{6, 114}",
            "box.space.byte_vals:insert{7, -89}",
            "box.space.byte_vals:insert{8, 300}",
            "box.space.byte_vals:insert{9, -250}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("byte_vals", "pk"));

        resultSet.next();
        assertEquals(-128, resultSet.getByte(1));

        resultSet.next();
        assertEquals(127, resultSet.getByte(1));

        // last nil value is trimmed
        resultSet.next();
        assertEquals(1, resultSet.getRowSize());

        resultSet.next();
        assertEquals(0, resultSet.getByte(1));
        assertFalse(resultSet.isNull(1));

        resultSet.next();
        assertEquals(15, resultSet.getByte(1));

        resultSet.next();
        assertEquals(114, resultSet.getByte(1));

        resultSet.next();
        assertEquals(-89, resultSet.getByte(1));

        resultSet.next();
        assertThrows(TarantoolClientException.class, () -> resultSet.getByte(1));

        resultSet.next();
        assertThrows(TarantoolClientException.class, () -> resultSet.getByte(1));

        dropSpace("byte_vals");
    }

    @Test
    void testGetShortValue() {
        testHelper.executeLua(
            "box.schema.space.create('short_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'byte_val', type = 'integer', is_nullable = true} }" +
                "})",
            "box.space.short_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.short_vals:insert{1, -32768}",
            "box.space.short_vals:insert{2, 32767}",
            "box.space.short_vals:insert{4, 0}",
            "box.space.short_vals:insert{5, -1}",
            "box.space.short_vals:insert{6, 12843}",
            "box.space.short_vals:insert{7, -7294}",
            "box.space.short_vals:insert{8, 34921}",
            "box.space.short_vals:insert{9, -37123}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("short_vals", "pk"));

        resultSet.next();
        assertEquals(-32768, resultSet.getShort(1));

        resultSet.next();
        assertEquals(32767, resultSet.getShort(1));

        resultSet.next();
        assertEquals(0, resultSet.getShort(1));
        assertFalse(resultSet.isNull(1));

        resultSet.next();
        assertEquals(-1, resultSet.getShort(1));

        resultSet.next();
        assertEquals(12843, resultSet.getShort(1));

        resultSet.next();
        assertEquals(-7294, resultSet.getShort(1));

        resultSet.next();
        assertThrows(TarantoolClientException.class, () -> resultSet.getShort(1));

        resultSet.next();
        assertThrows(TarantoolClientException.class, () -> resultSet.getShort(1));

        dropSpace("short_vals");
    }

    @Test
    void testGetIntValue() {
        testHelper.executeLua(
            "box.schema.space.create('int_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'int_vals', type = 'integer', is_nullable = true} }" +
                "})",
            "box.space.int_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.int_vals:insert{1, -2147483648}",
            "box.space.int_vals:insert{2, 2147483647}",
            "box.space.int_vals:insert{4, 0}",
            "box.space.int_vals:insert{5, -134}",
            "box.space.int_vals:insert{6, 589213}",
            "box.space.int_vals:insert{7, -1234987}",
            "box.space.int_vals:insert{8, 3897234258}",
            "box.space.int_vals:insert{9, -2289123645}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("int_vals", "pk"));

        resultSet.next();
        assertEquals(-2147483648, resultSet.getInt(1));

        resultSet.next();
        assertEquals(2147483647, resultSet.getInt(1));

        resultSet.next();
        assertEquals(0, resultSet.getInt(1));
        assertFalse(resultSet.isNull(1));

        resultSet.next();
        assertEquals(-134, resultSet.getInt(1));

        resultSet.next();
        assertEquals(589213, resultSet.getInt(1));

        resultSet.next();
        assertEquals(-1234987, resultSet.getInt(1));

        resultSet.next();
        assertThrows(TarantoolClientException.class, () -> resultSet.getInt(1));

        resultSet.next();
        assertThrows(TarantoolClientException.class, () -> resultSet.getInt(1));

        dropSpace("int_vals");
    }

    @Test
    void testGetLongValue() {
        testHelper.executeLua(
            "box.schema.space.create('long_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'long_val', type = 'integer', is_nullable = true} }" +
                "})",
            "box.space.long_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.long_vals:insert{1, -9223372036854775808LL}",
            "box.space.long_vals:insert{2, 9223372036854775807LL}",
            "box.space.long_vals:insert{3, 0LL}",
            "box.space.long_vals:insert{4, -89123LL}",
            "box.space.long_vals:insert{5, 2183428734598754LL}",
            "box.space.long_vals:insert{6, -918989823492348843LL}",
            "box.space.long_vals:insert{7, 18446744073709551615ULL}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("long_vals", "pk"));

        resultSet.next();
        assertEquals(-9223372036854775808L, resultSet.getLong(1));

        resultSet.next();
        assertEquals(9223372036854775807L, resultSet.getLong(1));

        resultSet.next();
        assertEquals(0, resultSet.getLong(1));
        assertFalse(resultSet.isNull(1));

        resultSet.next();
        assertEquals(-89123, resultSet.getLong(1));

        resultSet.next();
        assertEquals(2183428734598754L, resultSet.getLong(1));

        resultSet.next();
        assertEquals(-918989823492348843L, resultSet.getLong(1));

        resultSet.next();
        assertThrows(TarantoolClientException.class, () -> resultSet.getLong(1));

        dropSpace("long_vals");
    }

    @Test
    void testGetFloatValue() {
        testHelper.executeLua(
            "box.schema.space.create('float_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'float_val', type = 'number', is_nullable = true} }" +
                "})",
            "box.space.float_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.float_vals:insert{1, -12.6}",
            "box.space.float_vals:insert{2, 14.098}",
            "box.space.float_vals:insert{3, 0}",
            "box.space.float_vals:insert{4, -1230988}",
            "box.space.float_vals:insert{5, 213124234}",
            "box.space.float_vals:insert{6, -78.0}",
            "box.space.float_vals:insert{7, 97.14827}"
        );
        final float delta = 1e-9f;

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("float_vals", "pk"));

        resultSet.next();
        assertEquals(-12.6f, resultSet.getFloat(1), delta);

        resultSet.next();
        assertEquals(14.098f, resultSet.getFloat(1), delta);

        resultSet.next();
        assertEquals(0, resultSet.getFloat(1));

        resultSet.next();
        assertEquals(-1230988f, resultSet.getFloat(1));

        resultSet.next();
        assertEquals(213124234f, resultSet.getFloat(1));

        resultSet.next();
        assertEquals(-78.0f, resultSet.getFloat(1));

        resultSet.next();
        assertEquals(97.14827f, resultSet.getFloat(1), delta);

        dropSpace("float_vals");
    }

    @Test
    void testGetDoubleValue() {
        testHelper.executeLua(
            "box.schema.space.create('double_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'double_val', type = 'number', is_nullable = true} }" +
                "})",
            "box.space.double_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.double_vals:insert{1, -12.60}",
            "box.space.double_vals:insert{2, 43.9093}",
            "box.space.double_vals:insert{3, 0}",
            "box.space.double_vals:insert{4, -89234234}",
            "box.space.double_vals:insert{5, 532982423}",
            "box.space.double_vals:insert{6, -134.0}",
            "box.space.double_vals:insert{7, 4232.8264286}"
        );
        final double delta = 1e-9;

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("double_vals", "pk"));

        resultSet.next();
        assertEquals(-12.60, resultSet.getDouble(1), delta);

        resultSet.next();
        assertEquals(43.9093, resultSet.getDouble(1), delta);

        resultSet.next();
        assertEquals(0, resultSet.getDouble(1));

        resultSet.next();
        assertEquals(-89234234, resultSet.getDouble(1));

        resultSet.next();
        assertEquals(532982423, resultSet.getDouble(1));

        resultSet.next();
        assertEquals(-134.0f, resultSet.getDouble(1));

        resultSet.next();
        assertEquals(4232.8264286, resultSet.getDouble(1), delta);

        dropSpace("double_vals");
    }

    @Test
    void testGetBooleanValue() {
        testHelper.executeLua(
            "box.schema.space.create('bool_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'bool_val', type = 'boolean', is_nullable = true} }" +
                "})",
            "box.space.bool_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.bool_vals:insert{1, true}",
            "box.space.bool_vals:insert{2, false}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("bool_vals", "pk"));

        resultSet.next();
        assertTrue(resultSet.getBoolean(1));

        resultSet.next();
        assertFalse(resultSet.getBoolean(1));

        dropSpace("bool_vals");
    }

    @Test
    void testGetBooleanFromNumber() {
        testHelper.executeLua(
            "box.schema.space.create('bool_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'bool_val', type = 'number', is_nullable = true} }" +
                "})",
            "box.space.bool_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.bool_vals:insert{1, 0}",
            "box.space.bool_vals:insert{2, 1}",
            "box.space.bool_vals:insert{3, 1.0}",
            "box.space.bool_vals:insert{4, 0.0}",
            "box.space.bool_vals:insert{5, 4}",
            "box.space.bool_vals:insert{6, -132}",
            "box.space.bool_vals:insert{7, 242.234}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("bool_vals", "pk"));

        resultSet.next();
        assertFalse(resultSet.getBoolean(1));

        resultSet.next();
        assertTrue(resultSet.getBoolean(1));

        resultSet.next();
        assertTrue(resultSet.getBoolean(1));

        resultSet.next();
        assertFalse(resultSet.getBoolean(1));

        resultSet.next();
        assertThrows(TarantoolClientException.class, () -> resultSet.getBoolean(1));

        resultSet.next();
        assertThrows(TarantoolClientException.class, () -> resultSet.getBoolean(1));

        resultSet.next();
        assertThrows(TarantoolClientException.class, () -> resultSet.getBoolean(1));

        dropSpace("bool_vals");
    }

    @Test
    void testGetBooleanFromString() {
        testHelper.executeLua(
            "box.schema.space.create('bool_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'bool_val', type = 'string', is_nullable = true} }" +
                "})",
            "box.space.bool_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.bool_vals:insert{1, '0'}",
            "box.space.bool_vals:insert{2, '1'}",
            "box.space.bool_vals:insert{3, 'on'}",
            "box.space.bool_vals:insert{4, 'off'}",
            "box.space.bool_vals:insert{5, 'true'}",
            "box.space.bool_vals:insert{6, 't'}",
            "box.space.bool_vals:insert{7, 'false'}",
            "box.space.bool_vals:insert{8, 'f'}",
            "box.space.bool_vals:insert{9, 'yes'}",
            "box.space.bool_vals:insert{10, 'y'}",
            "box.space.bool_vals:insert{11, 'no'}",
            "box.space.bool_vals:insert{12, 'n'}",
            "box.space.bool_vals:insert{13, ' '}",
            "box.space.bool_vals:insert{14, 'some string'}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("bool_vals", "pk"));

        resultSet.next();
        assertFalse(resultSet.getBoolean(1));

        resultSet.next();
        assertTrue(resultSet.getBoolean(1));

        resultSet.next();
        assertTrue(resultSet.getBoolean(1));

        resultSet.next();
        assertFalse(resultSet.getBoolean(1));

        resultSet.next();
        assertTrue(resultSet.getBoolean(1));

        resultSet.next();
        assertTrue(resultSet.getBoolean(1));

        resultSet.next();
        assertFalse(resultSet.getBoolean(1));

        resultSet.next();
        assertFalse(resultSet.getBoolean(1));

        resultSet.next();
        assertTrue(resultSet.getBoolean(1));

        resultSet.next();
        assertTrue(resultSet.getBoolean(1));

        resultSet.next();
        assertFalse(resultSet.getBoolean(1));

        resultSet.next();
        assertFalse(resultSet.getBoolean(1));

        resultSet.next();
        assertThrows(TarantoolClientException.class, () -> resultSet.getBoolean(1));

        resultSet.next();
        assertThrows(TarantoolClientException.class, () -> resultSet.getBoolean(1));

        dropSpace("bool_vals");
    }

    @Test
    void testGetBytesValue() {
        testHelper.executeLua(
            "box.schema.space.create('bin_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'bin_val', type = 'scalar', is_nullable = true} }" +
                "})",
            "box.space.bin_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.bin_vals:insert{1, [[some text]]}",
            "box.space.bin_vals:insert{2, '\\01\\02\\03\\04\\05'}",
            "box.space.bin_vals:insert{2, 12}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("bin_vals", "pk"));

        resultSet.next();
        assertArrayEquals("some text".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(1));

        resultSet.next();
        assertArrayEquals(new byte[] { 0x1, 0x2, 0x3, 0x4, 0x5 }, resultSet.getBytes(1));

        resultSet.next();
        assertThrows(TarantoolClientException.class, () -> resultSet.getBoolean(1));

        dropSpace("bin_vals");
    }

    @Test
    void testGetStringValue() {
        testHelper.executeLua(
            "box.schema.space.create('string_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'text_val', type = 'string', is_nullable = true} }" +
                "})",
            "box.space.string_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.string_vals:insert{1, 'some text'}",
            "box.space.string_vals:insert{2, 'word'}",
            "box.space.string_vals:insert{3, ''}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("string_vals", "pk"));

        resultSet.next();
        assertEquals("some text", resultSet.getString(1));

        resultSet.next();
        assertEquals("word", resultSet.getString(1));

        resultSet.next();
        assertEquals("", resultSet.getString(1));

        dropSpace("string_vals");
    }

    @Test
    void testGetWrongStringValue() {
        TarantoolResultSet resultSet = client.executeRequest(Requests.evalRequest(
            "return {a=1,b=2}, {1,2,3}"
        ));

        resultSet.next();
        assertThrows(TarantoolClientException.class, () -> resultSet.getString(0));
        assertThrows(TarantoolClientException.class, () -> resultSet.getString(1));
    }

    @Test
    void testGetStringFromScalar() {
        testHelper.executeLua(
            "box.schema.space.create('scalar_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'scalar_val', type = 'scalar', is_nullable = true} }" +
                "})",
            "box.space.scalar_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.scalar_vals:insert{1, 'some text'}",
            "box.space.scalar_vals:insert{2, 12}",
            "box.space.scalar_vals:insert{3, 12.45}",
            "box.space.scalar_vals:insert{4, '\\01\\02\\03'}",
            "box.space.scalar_vals:insert{5, true}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("scalar_vals", "pk"));

        resultSet.next();
        assertEquals("some text", resultSet.getString(1));

        resultSet.next();
        assertEquals("12", resultSet.getString(1));

        resultSet.next();
        assertEquals("12.45", resultSet.getString(1));

        resultSet.next();
        assertEquals(new String(new byte[] { 0x1, 0x2, 0x3 }), resultSet.getString(1));

        resultSet.next();
        assertEquals("true", resultSet.getString(1));

        dropSpace("scalar_vals");
    }

    @Test
    void testGetObject() {
        testHelper.executeLua(
            "box.schema.space.create('object_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'object_val', type = 'scalar', is_nullable = true} }" +
                "})",
            "box.space.object_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.object_vals:insert{1, 'some text'}",
            "box.space.object_vals:insert{2, 12}",
            "box.space.object_vals:insert{3, 12.45}",
            "box.space.object_vals:insert{4, '\\01\\02\\03'}",
            "box.space.object_vals:insert{5, true}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("object_vals", "pk"));

        resultSet.next();
        assertEquals("some text", resultSet.getObject(1));

        resultSet.next();
        assertEquals(12, resultSet.getObject(1));

        resultSet.next();
        assertEquals(12.45, (double) resultSet.getObject(1), 1e-9);

        resultSet.next();
        assertEquals(new String(new byte[] { 0x1, 0x2, 0x3 }), resultSet.getObject(1));

        resultSet.next();
        assertTrue((boolean) resultSet.getObject(1));

        dropSpace("object_vals");
    }

    @Test
    void testGetStringFromObjectMapping() {
        testHelper.executeLua(
            "box.schema.space.create('object_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'object_val', type = 'scalar', is_nullable = true} }" +
                "})",
            "box.space.object_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.object_vals:insert{1, 'some text'}",
            "box.space.object_vals:insert{2, 12}",
            "box.space.object_vals:insert{3, 500000000000}",
            "box.space.object_vals:insert{4, 12.45}",
            "box.space.object_vals:insert{5, '\\01\\02\\03'}",
            "box.space.object_vals:insert{6, true}"
        );

        Map<Class<?>, Function<Object, Object>> mappers = new HashMap<>();
        mappers.put(
            String.class,
            v -> ((String) v).toUpperCase()
        );
        mappers.put(
            Integer.class,
            v -> String.format("%( 5d", (int) v)
        );
        mappers.put(
            Double.class,
            v -> String.format("%05.1f", (double) v)
        );
        mappers.put(
            Boolean.class,
            v -> (boolean) v ? "yes" : "no"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("object_vals", "pk"));

        resultSet.next();
        assertEquals("SOME TEXT", resultSet.getObject(1, mappers));

        resultSet.next();
        assertEquals("   12", resultSet.getObject(1, mappers));

        resultSet.next();
        assertEquals(500000000000L, resultSet.getObject(1, mappers));

        resultSet.next();
        assertEquals("012.5", resultSet.getObject(1, mappers));

        resultSet.next();
        assertEquals(new String(new byte[] { 0x1, 0x2, 0x3 }), resultSet.getObject(1, mappers));

        resultSet.next();
        assertEquals("yes", resultSet.getObject(1, mappers));

        dropSpace("object_vals");
    }

    @Test
    void testGetBigInteger() {
        testHelper.executeLua(
            "box.schema.space.create('bigint_vals', { format = " +
                "{{name = 'id', type = 'integer'}, {name = 'bigint_val', type = 'integer', is_nullable = true} }" +
                "})",
            "box.space.bigint_vals:create_index('pk', { type = 'TREE', parts = {'id'} } )",

            "box.space.bigint_vals:insert{1, 10001}",
            "box.space.bigint_vals:insert{2, 12}",
            "box.space.bigint_vals:insert{3, 0}",
            "box.space.bigint_vals:insert{4, 18446744073709551615ULL}",
            "box.space.bigint_vals:insert{5, -4792}",
            "box.space.bigint_vals:insert{6, -9223372036854775808LL}"
        );

        TarantoolResultSet resultSet = client.executeRequest(Requests.selectRequest("bigint_vals", "pk"));

        resultSet.next();
        assertEquals(new BigInteger("10001"), resultSet.getBigInteger(1));

        resultSet.next();
        assertEquals(new BigInteger("12"), resultSet.getBigInteger(1));

        resultSet.next();
        assertEquals(new BigInteger("0"), resultSet.getBigInteger(1));

        resultSet.next();
        assertEquals(new BigInteger("18446744073709551615"), resultSet.getBigInteger(1));

        resultSet.next();
        assertEquals(new BigInteger("-4792"), resultSet.getBigInteger(1));

        resultSet.next();
        assertEquals(new BigInteger("-9223372036854775808"), resultSet.getBigInteger(1));

        dropSpace("bigint_vals");
    }

    @Test
    void testGetList() {
        TarantoolResultSet resultSet = client.executeRequest(
            Requests.evalRequest("return {'a','b','c'}, {1,2,3}, nil, {}, 'string'")
        );

        assertEquals(1, resultSet.size());

        resultSet.next();
        assertEquals(5, resultSet.getRowSize());
        assertEquals(Arrays.asList("a", "b", "c"), resultSet.getList(0));
        assertEquals(Arrays.asList(1, 2, 3), resultSet.getList(1));
        assertNull(resultSet.getList(2));
        assertEquals(Collections.emptyList(), resultSet.getList(3));
        assertThrows(TarantoolClientException.class, () -> resultSet.getList(4));
    }

    @Test
    void testGetMap() {
        TarantoolResultSet resultSet = client.executeRequest(
            Requests.evalRequest(
                "return {a=1,b=2}, {['key 1']=8,['key 2']=9}, {['+']='add',['/']='div'}, 'string'"
            )
        );

        assertEquals(1, resultSet.size());

        resultSet.next();
        assertEquals(4, resultSet.getRowSize());
        assertEquals(
            new HashMap<Object, Object>() {
                {
                    put("a", 1);
                    put("b", 2);
                }
            },
            resultSet.getMap(0));
        assertEquals(
            new HashMap<Object, Object>() {
                {
                    put("key 1", 8);
                    put("key 2", 9);
                }
            },
            resultSet.getMap(1));
        assertEquals(
            new HashMap<Object, Object>() {
                {
                    put("+", "add");
                    put("/", "div");
                }
            },
            resultSet.getMap(2));
        assertThrows(TarantoolClientException.class, () -> resultSet.getMap(3));
    }

    @Test
    void testGetMixedValues() {
        testHelper.executeLua(
            "function getMixed() return 10, -20.5, 'string', nil, true, {1,2,3}, {x='f',y='t'} end"
        );
        TarantoolResultSet resultSet = client.executeRequest(Requests.callRequest("getMixed"));

        assertEquals(1, resultSet.size());

        resultSet.next();
        assertEquals(7, resultSet.getRowSize());
        assertEquals(10, resultSet.getInt(0));
        assertEquals(-20.5, resultSet.getDouble(1), 1e-9);
        assertEquals("string", resultSet.getString(2));
        assertTrue(resultSet.isNull(3));
        assertTrue(resultSet.getBoolean(4));
        assertEquals(Arrays.asList(1, 2, 3), resultSet.getList(5));
        assertEquals(
            new HashMap<Object, Object>() {
                {
                    put("x", "f");
                    put("y", "t");
                }
            },
            resultSet.getMap(6));
    }

    @Test
    void testGetNullValues() {
        testHelper.executeLua(
            "function getNull() return nil end"
        );
        TarantoolResultSet resultSet = client.executeRequest(Requests.callRequest("getNull"));

        resultSet.next();
        assertFalse(resultSet.getBoolean(0));
        assertEquals(0, resultSet.getByte(0));
        assertEquals(0, resultSet.getShort(0));
        assertEquals(0, resultSet.getInt(0));
        assertEquals(0, resultSet.getLong(0));
        assertNull(resultSet.getBigInteger(0));
        assertEquals(0.0, resultSet.getFloat(0));
        assertEquals(0.0, resultSet.getDouble(0));
        assertNull(resultSet.getList(0));
        assertNull(resultSet.getMap(0));
        assertNull(resultSet.getObject(0));
        assertNull(resultSet.getObject(0, Collections.emptyMap()));
        assertNull(resultSet.getString(0));
        assertNull(resultSet.getBytes(0));
    }

    @Test
    void testGetNumberFromString() {
        testHelper.executeLua(
            "function getString() return '120.5' end"
        );
        TarantoolResultSet resultSet = client.executeRequest(Requests.callRequest("getString"));

        resultSet.next();
        assertEquals(120, resultSet.getByte(0));
        assertEquals(120, resultSet.getShort(0));
        assertEquals(120, resultSet.getInt(0));
        assertEquals(120, resultSet.getLong(0));
        assertEquals(BigInteger.valueOf(120), resultSet.getBigInteger(0));
        assertEquals(120.5, resultSet.getFloat(0));
        assertEquals(120.5, resultSet.getDouble(0));
    }

    @Test
    void testGetNotParsableNumberFromString() {
        testHelper.executeLua(
            "function getString() return 'five point six' end"
        );
        TarantoolResultSet resultSet = client.executeRequest(Requests.callRequest("getString"));

        resultSet.next();
        assertThrows(TarantoolClientException.class, () -> resultSet.getByte(0));
        assertThrows(TarantoolClientException.class, () -> resultSet.getShort(0));
        assertThrows(TarantoolClientException.class, () -> resultSet.getInt(0));
        assertThrows(TarantoolClientException.class, () -> resultSet.getLong(0));
        assertThrows(TarantoolClientException.class, () -> resultSet.getBigInteger(0));
        assertThrows(TarantoolClientException.class, () -> resultSet.getFloat(0));
        assertThrows(TarantoolClientException.class, () -> resultSet.getDouble(0));
    }

    @Test
    void testGetTooLargeNumberValue() {
        testHelper.executeLua(
            "function getNumber() return 300, 100000, 5000000000, 10000000000000000000ULL end",
            "function getString() return '300', '100000', '5000000000', '10000000000000000000' end"
        );
        TarantoolResultSet numberResult = client.executeRequest(Requests.callRequest("getNumber"));

        numberResult.next();
        assertThrows(TarantoolClientException.class, () -> numberResult.getByte(0));
        assertThrows(TarantoolClientException.class, () -> numberResult.getShort(1));
        assertThrows(TarantoolClientException.class, () -> numberResult.getInt(2));
        assertThrows(TarantoolClientException.class, () -> numberResult.getLong(3));

        TarantoolResultSet stringResult = client.executeRequest(Requests.callRequest("getString"));

        stringResult.next();
        assertThrows(TarantoolClientException.class, () -> stringResult.getByte(0));
        assertThrows(TarantoolClientException.class, () -> stringResult.getShort(1));
        assertThrows(TarantoolClientException.class, () -> stringResult.getInt(2));
        assertThrows(TarantoolClientException.class, () -> stringResult.getLong(3));
    }

    private void dropSpace(String spaceName) {
        testHelper.executeLua(String.format("box.space.%1$s and box.space.%1$s:drop()", spaceName));
    }
}
