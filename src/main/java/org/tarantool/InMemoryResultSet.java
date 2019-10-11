package org.tarantool;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Simple implementation of {@link TarantoolResultSet}
 * that contains all tuples in a local memory.
 */
class InMemoryResultSet implements TarantoolResultSet {

    private static final BigInteger BYTE_MAX = new BigInteger(Byte.toString(Byte.MAX_VALUE));
    private static final BigInteger BYTE_MIN = new BigInteger(Byte.toString(Byte.MIN_VALUE));

    private static final BigInteger SHORT_MAX = new BigInteger(Short.toString(Short.MAX_VALUE));
    private static final BigInteger SHORT_MIN = new BigInteger(Short.toString(Short.MIN_VALUE));

    private static final BigInteger INT_MAX = new BigInteger(Integer.toString(Integer.MAX_VALUE));
    private static final BigInteger INT_MIN = new BigInteger(Integer.toString(Integer.MIN_VALUE));

    private static final BigInteger LONG_MAX = new BigInteger(Long.toString(Long.MAX_VALUE));
    private static final BigInteger LONG_MIN = new BigInteger(Long.toString(Long.MIN_VALUE));

    private List<Object> results;

    private int currentIndex;
    private List<?> currentTuple;

    InMemoryResultSet(List<?> rawResult, boolean asSingleResult) {
        currentIndex = -1;
        results = new ArrayList<>();

        ArrayList<Object> copiedResult = new ArrayList<>(rawResult);
        if (asSingleResult) {
            results.add(copiedResult);
        } else {
            this.results.addAll(copiedResult);
        }
    }

    @Override
    public boolean next() {
        if ((currentIndex + 1) < results.size()) {
            currentTuple = getAsTuple(++currentIndex);
            return true;
        }
        return false;
    }

    @Override
    public boolean previous() {
        if ((currentIndex - 1) >= 0) {
            currentTuple = getAsTuple(--currentIndex);
            return true;
        }
        return false;
    }

    @Override
    public byte getByte(int columnIndex) {
        checkTupleSize(currentTuple, columnIndex);
        Object value = currentTuple.get(columnIndex);
        if (value == null) {
            return 0;
        }
        if (value instanceof Byte) {
            return (byte) value;
        }
        if (value instanceof Number) {
            return (byte) asLong((Number) value, Byte.MIN_VALUE, Byte.MAX_VALUE, "byte");
        }
        return (byte) castToLong(getString(columnIndex), Byte::parseByte, BYTE_MIN, BYTE_MAX, "byte");
    }

    @Override
    public short getShort(int columnIndex) {
        checkTupleSize(currentTuple, columnIndex);
        Object value = currentTuple.get(columnIndex);
        if (value == null) {
            return 0;
        }
        if (value instanceof Short) {
            return (short) value;
        }
        if (value instanceof Number) {
            return (short) asLong((Number) value, Short.MIN_VALUE, Short.MAX_VALUE, "short");
        }
        return (short) castToLong(getString(columnIndex), Short::parseShort, SHORT_MIN, SHORT_MAX, "short");
    }

    @Override
    public int getInt(int columnIndex) {
        checkTupleSize(currentTuple, columnIndex);
        Object value = currentTuple.get(columnIndex);
        if (value == null) {
            return 0;
        }
        if (value instanceof Integer) {
            return (int) value;
        }
        if (value instanceof Number) {
            return (int) asLong((Number) value, Integer.MIN_VALUE, Integer.MAX_VALUE, "int");
        }
        return (int) castToLong(getString(columnIndex), Integer::parseInt, INT_MIN, INT_MAX, "int");
    }

    @Override
    public long getLong(int columnIndex) {
        checkTupleSize(currentTuple, columnIndex);
        Object value = currentTuple.get(columnIndex);
        if (value == null) {
            return 0;
        }
        if (value instanceof Long) {
            return (long) value;
        }
        if (value instanceof Number) {
            return asLong((Number) value, Long.MIN_VALUE, Long.MAX_VALUE, "long");
        }
        return castToLong(getString(columnIndex), Long::parseLong, LONG_MIN, LONG_MAX, "long");

    }

    @Override
    public float getFloat(int columnIndex) {
        checkTupleSize(currentTuple, columnIndex);
        Object value = currentTuple.get(columnIndex);
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        return (float) castToDouble(value.toString(), "float");
    }

    @Override
    public double getDouble(int columnIndex) {
        checkTupleSize(currentTuple, columnIndex);
        Object value = currentTuple.get(columnIndex);
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return castToDouble(value.toString(), "double");
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        checkTupleSize(currentTuple, columnIndex);
        Object value = currentTuple.get(columnIndex);
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return (boolean) value;
        }
        if (value instanceof Number) {
            double number = ((Number) value).doubleValue();
            if (number == 1.0d) {
                return true;
            }
            if (number == 0.0d) {
                return false;
            }
            throw new TarantoolClientException("Numbers rather than 1 or 0 cannot be cast to a boolean value");
        }
        return castToBoolean(getString(columnIndex));
    }

    @Override
    public byte[] getBytes(int columnIndex) {
        checkTupleSize(currentTuple, columnIndex);
        Object value = currentTuple.get(columnIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof String) {
            return ((String) value).getBytes(StandardCharsets.UTF_8);
        }
        throw new TarantoolClientException(value.getClass().getName() + " value cannot be cast to bytes array");
    }

    @Override
    public String getString(int columnIndex) {
        checkTupleSize(currentTuple, columnIndex);
        Object value = currentTuple.get(columnIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        return castToString(value);
    }

    @Override
    public Object getObject(int columnIndex) {
        checkTupleSize(currentTuple, columnIndex);
        return currentTuple.get(columnIndex);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object getObject(int columnIndex, Map<Class<?>, Function<Object, Object>> mappers) {
        checkTupleSize(currentTuple, columnIndex);
        Object value = currentTuple.get(columnIndex);
        if (value == null) {
            return null;
        }
        Function<Object, Object> mapper = mappers.get(value.getClass());
        if (mapper != null) {
            return mapper.apply(value);
        }
        return value;
    }

    @Override
    public BigInteger getBigInteger(int columnIndex) {
        checkTupleSize(currentTuple, columnIndex);
        Object value = currentTuple.get(columnIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        }
        try {
            String stringValue = getString(columnIndex);
            return new BigDecimal(stringValue).toBigInteger();
        } catch (NumberFormatException ignored) {
            throw new TarantoolClientException("Value " + value + " cannot be parsed as BigInteger");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Object> getList(int columnIndex) {
        checkTupleSize(currentTuple, columnIndex);
        Object value = currentTuple.get(columnIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof List<?>) {
            return (List<Object>) value;
        }
        throw new TarantoolClientException(value.getClass().getName() + " value cannot be cast to a List");
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<Object, Object> getMap(int columnIndex) {
        checkTupleSize(currentTuple, columnIndex);
        Object value = currentTuple.get(columnIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?>) {
            return (Map<Object, Object>) value;
        }
        throw new TarantoolClientException(value.getClass().getName() + " value cannot be cast to a Map");
    }

    @Override
    public boolean isNull(int columnIndex) {
        checkTupleSize(currentTuple, columnIndex);
        Object value = currentTuple.get(columnIndex);
        return value == null;
    }

    @Override
    public int getRow() {
        return currentIndex;
    }

    @Override
    public int getRowSize() {
        return (currentTuple != null) ? currentTuple.size() : -1;
    }

    @Override
    public boolean isEmpty() {
        return results.isEmpty();
    }

    @Override
    public int size() {
        return results.size();
    }

    @Override
    public void close() {
        results.clear();
        currentTuple = null;
        currentIndex = -1;
    }

    private List<?> getAsTuple(int index) {
        Object row = results.get(index);
        try {
            return (List<?>) row;
        } catch (ClassCastException e) {
            throw new TarantoolClientException("Current row has type " +
                row.getClass().getName() +
                " and cannot be accessed using an index number");
        }
    }

    private void checkTupleSize(List<?> tuple, int index) {
        if (index < 0 || index > tuple.size()) {
            throw new TarantoolClientException(index + "is out of tuple size " + tuple.size());
        }
    }

    private long asLong(Number value, long min, long max, String type) {
        long number = 0;
        try {
            number = value instanceof BigInteger ? ((BigInteger) value).longValueExact() : value.longValue();
        } catch (ArithmeticException ignored) {
            throw new TarantoolClientException(
                "Value " + value + " is out of " + type + " range [" + min + ".." + max + "]"
            );
        }
        if (number < min || number > max) {
            throw new TarantoolClientException(
                "Value " + number + " is out of " + type + " range [" + min + ".." + max + "]"
            );
        }
        return number;
    }

    private long castToLong(String value,
                            Function<String, Number> parser,
                            BigInteger min,
                            BigInteger max,
                            String type) {
        try {
            return parser.apply(value.trim()).longValue();
        } catch (NumberFormatException ignored) {
            try {
                BigInteger intValue = new BigDecimal(value).toBigInteger();
                if (intValue.compareTo(max) > 0 || intValue.compareTo(min) < 0) {
                    throw new TarantoolClientException(
                        "Value " + intValue + " is out of " + type + " range [" + min + ".." + max + "]"
                    );
                }
                return intValue.longValue();
            } catch (NumberFormatException ex) {
                throw new TarantoolClientException("Value " + value + " cannot be parsed as " + type);
            }
        }
    }

    private double castToDouble(String value, String type) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException ignored) {
            throw new TarantoolClientException("Value " + value + " cannot be parsed as " + type);
        }
    }

    private boolean castToBoolean(String value) {
        if ("1".equals(value) || "on".equalsIgnoreCase(value) ||
            "true".equalsIgnoreCase(value) || "t".equalsIgnoreCase(value) ||
            "yes".equalsIgnoreCase(value) || "y".equalsIgnoreCase(value)) {
            return true;
        }
        if ("0".equals(value) || "off".equalsIgnoreCase(value) ||
            "false".equalsIgnoreCase(value) || "f".equalsIgnoreCase(value) ||
            "no".equalsIgnoreCase(value) || "n".equalsIgnoreCase(value)) {
            return false;
        }
        throw new TarantoolClientException(value.getClass().getName() + " value cannot be cast to a boolean value");
    }

    private String castToString(Object value) {
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            return new String(bytes, StandardCharsets.UTF_8);
        }
        throw new TarantoolClientException(value.getClass().getName() + " value cannot be cast to a string value");
    }
}
