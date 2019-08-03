package org.tarantool.jdbc;

import static org.tarantool.jdbc.EscapedFunctions.Expression;
import static org.tarantool.jdbc.EscapedFunctions.FunctionExpression;
import static org.tarantool.jdbc.EscapedFunctions.FunctionSignatureKey;
import static org.tarantool.jdbc.EscapedFunctions.functionMappings;

import org.tarantool.util.SQLStates;
import org.tarantool.util.ThrowingBiFunction;

import java.sql.Connection;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Set of utils to work with JDBC escape processing.
 * <p>
 * Supported escape syntax:
 * <ol>
 *  <li>Scalar functions (i.e. {@code {fn random()}}).</li>
 *  <li>Outer joins (i.e. {@code {oj "dept" left outer join "salary" on "dept_id" = 1412}}).</li>
 *  <li>Like escape character (i.e. {@code like '_|%_3%' {escape '|'}}).</li>
 *  <li>Limiting returned rows (i.e. {@code {limit 10 offset 20}}).</li>
 * </ol>
 * <p>
 * Most of the supported expressions translates directly omitting escape borders.
 * In this way, {@code {fn abs(-5)}} becomes {@code abs(-5)}} or {@code {limit 10 offset 50}}
 * becomes {@code limit 10 offset 50} and so on. There are exceptions in case of scalar
 * functions where JDBC functions may not match exactly with Tarantool ones (for example,
 * JDBC {@code {fn rand()}} function becomes {@code random()} supported by Tarantool.
 */
public class EscapeSyntaxParser {

    private static final Pattern IDENTIFIER = Pattern.compile("[_a-zA-Z][_a-zA-Z0-9]+");

    private final SQLConnection jdbcContext;

    public EscapeSyntaxParser(SQLConnection jdbcContext) {
        this.jdbcContext = jdbcContext;
    }

    /**
     * Performs escape processing for SQL queries. It translates
     * sql text with optional escape expressions such as {@code {fn abs(-1)}}.
     *
     * <p>
     * Comments inside SQL text can be eliminated as parsing goes using preserveComments
     * flag. Hence, Comments inside escape syntax are always omitted regardless of
     * the flag, though.
     *
     * @param sql              SQL text to be processed
     * @param preserveComments flag indicating should comments be kept
     *
     * @return native SQL query
     *
     * @throws SQLSyntaxErrorException if any syntax error happened
     */
    public String translate(String sql, boolean preserveComments) throws SQLSyntaxErrorException {
        StringBuilder nativeSql = new StringBuilder(sql.length());
        StringBuilder escapeBuffer = new StringBuilder();
        StringBuilder activeBuffer = nativeSql;
        LinkedList<Integer> escapeStartPositions = new LinkedList<>();

        int i = 0;
        while (i < sql.length()) {
            char currentChar = sql.charAt(i);
            switch (currentChar) {
            case '\'':
            case '"':
                int endOfString = seekEndOfRegion(sql, i, "" + currentChar, "" + currentChar);
                if (endOfString == -1) {
                    throw new SQLSyntaxErrorException(
                        "Not enclosed string literal or quoted identifier at position " + i,
                        SQLStates.SYNTAX_ERROR.getSqlState()
                    );
                }
                activeBuffer.append(sql, i, endOfString + 1);
                i = endOfString + 1;
                break;

            case '/':
            case '-':
                int endOfComment;
                if (currentChar == '/') {
                    endOfComment = seekEndOfRegion(sql, i, "/*", "*/");
                    if (endOfComment == -1) {
                        throw new SQLSyntaxErrorException(
                            "Open block comment at position " + i, SQLStates.SYNTAX_ERROR.getSqlState()
                        );
                    }
                } else {
                    endOfComment = seekEndOfRegion(sql, i, "--", "\n");
                    if (endOfComment == -1) {
                        endOfComment = sql.length() - 1;
                    }
                }
                if (i == endOfComment) {
                    activeBuffer.append(currentChar);
                    i++;
                } else {
                    if (activeBuffer == nativeSql && preserveComments) {
                        nativeSql.append(sql, i, endOfComment + 1);
                    }
                    i = endOfComment + 1;
                }
                break;

            case '{':
                escapeStartPositions.addFirst(escapeBuffer.length());
                escapeBuffer.append(currentChar);
                activeBuffer = escapeBuffer;
                i++;
                break;

            case '}':
                Integer startPosition = escapeStartPositions.pollFirst();
                if (startPosition == null) {
                    throw new SQLSyntaxErrorException(
                        "Unexpected '}' at position " + i,
                        SQLStates.SYNTAX_ERROR.getSqlState()
                    );
                }
                escapeBuffer.append(currentChar);
                processEscapeExpression(escapeBuffer, startPosition, escapeBuffer.length());
                if (escapeStartPositions.isEmpty()) {
                    nativeSql.append(escapeBuffer);
                    escapeBuffer.setLength(0);
                    activeBuffer = nativeSql;
                }
                i++;
                break;

            default:
                activeBuffer.append(currentChar);
                i++;
                break;
            }
        }

        if (!escapeStartPositions.isEmpty()) {
            throw new SQLSyntaxErrorException(
                "Not enclosed escape expression at position " + escapeStartPositions.pollFirst(),
                SQLStates.SYNTAX_ERROR.getSqlState()
            );
        }
        return nativeSql.toString();
    }

    /**
     * Parses text like {@code functionName([arg[,args...]])}.
     * Arguments are not parsed recursively and saved as are.
     *
     * @param functionString text to be parsed
     *
     * @return parsing result containing function name and its parameters, if any
     *
     * @throws SQLSyntaxErrorException if any syntax errors happened
     */
    private FunctionExpression parseFunction(String functionString) throws SQLSyntaxErrorException {
        int braceNestLevel = 0;
        String functionName = null;
        List<String> functionParameters = new ArrayList<>();
        int parameterStartPosition = 0;

        int i = 0;
        while (i < functionString.length()) {
            char currentChar = functionString.charAt(i);
            switch (currentChar) {
            case '\'':
            case '"':
                i = seekEndOfRegion(functionString, i, "" + currentChar, "" + currentChar) + 1;
                break;

            case '/':
            case '-':
                int endOfComment = (currentChar == '/')
                    ? seekEndOfRegion(functionString, i, "/*", "*/")
                    : seekEndOfRegion(functionString, i, "--", "\n");
                i = endOfComment == -1 ? functionString.length() : endOfComment + 1;
                break;

            case '(':
                if (braceNestLevel++ == 0) {
                    // it's possible only one function opening brace
                    if (functionName != null) {
                        throw new SQLSyntaxErrorException(
                            "Malformed function expression " + functionString, SQLStates.SYNTAX_ERROR.getSqlState()
                        );
                    }
                    functionName = functionString.substring(0, i).trim().toUpperCase();
                    if (!IDENTIFIER.matcher(functionName).matches()) {
                        throw new SQLSyntaxErrorException(
                            "Invalid function identifier '" + functionName + "'", SQLStates.SYNTAX_ERROR.getSqlState()
                        );
                    }
                    parameterStartPosition = i + 1;
                }
                i++;
                break;

            case ')':
                if (--braceNestLevel == 0) {
                    // reach a function closing brace
                    // parse the last possible function parameter
                    String param = functionString.substring(parameterStartPosition, i).trim();
                    if (!functionParameters.isEmpty() || !param.isEmpty()) {
                        functionParameters.add(param);
                    }
                }
                i++;
                break;

            case ',':
                if (braceNestLevel == 1) {
                    // reach the function argument delimiter
                    // parse the argument before this comma
                    String param = functionString.substring(parameterStartPosition, i).trim();
                    parameterStartPosition = i + 1;
                    functionParameters.add(param);
                }
                i++;
                break;

            default:
                i++;
                break;
            }
        }

        if (functionName == null || braceNestLevel != 0) {
            throw new SQLSyntaxErrorException(
                "Malformed function expression '" + functionString + "'", SQLStates.SYNTAX_ERROR.getSqlState()
            );
        }
        return new FunctionExpression(functionName, functionParameters);
    }

    /**
     * Handles an escape expression.
     *
     * @param buffer buffer containing current escape expression, inclusive
     * @param start  start position of the escape syntax in the buffer, exclusive
     * @param end    end position of the escape syntax in the buffer
     *
     * @throws SQLSyntaxErrorException if any syntax error happened
     */
    private void processEscapeExpression(StringBuilder buffer, int start, int end)
        throws SQLSyntaxErrorException {
        if (buffer.charAt(start) != '{' || buffer.charAt(end - 1) != '}') {
            return;
        }
        int startExpression = seekFirstNonSpaceSymbol(buffer, start + 1);
        int endExpression = seekLastNonSpaceSymbol(buffer, end - 2);

        if (substringMatches(buffer, "fn ", startExpression)) {
            FunctionExpression expression = parseFunction(buffer.substring(startExpression + 3, endExpression));
            ThrowingBiFunction<FunctionExpression, Connection, Expression, SQLSyntaxErrorException> mapper =
                functionMappings.get(FunctionSignatureKey.of(expression.getName(), expression.getParameters().size()));
            if (mapper == null) {
                throw new SQLSyntaxErrorException(
                    "Unknown function " + expression.getName(),
                    SQLStates.SYNTAX_ERROR.getSqlState()
                );
            }
            buffer.replace(start, end, mapper.apply(expression, jdbcContext).toString());
        } else if (substringMatches(buffer, "oj ", startExpression)) {
            buffer.replace(start, end, buffer.substring(startExpression + 3, endExpression));
        } else if (substringMatches(buffer, "escape ", startExpression)) {
            buffer.replace(start, end, buffer.substring(startExpression, endExpression));
        } else if (substringMatches(buffer, "limit ", startExpression)) {
            buffer.replace(start, end, buffer.substring(startExpression, endExpression));
        } else {
            throw new SQLSyntaxErrorException("Unrecognizable escape expression", SQLStates.SYNTAX_ERROR.getSqlState());
        }
    }

    /**
     * Looks for the end of the region defined by its start and end
     * substring patterns.
     *
     * @param text        search text
     * @param position    start position in text to search the region, inclusive
     * @param startRegion pattern of the region start
     * @param endRegion   pattern of the region end
     *
     * @return found position of the region end, inclusive. Start position if the region start
     *     pattern does not match the text start position and {@literal -1} if the
     *     region end is not found.
     */
    private int seekEndOfRegion(String text, int position, String startRegion, String endRegion) {
        if (!text.regionMatches(position, startRegion, 0, startRegion.length())) {
            return position;
        }
        int end = text.indexOf(endRegion, position + startRegion.length());
        return end == -1 ? end : end + endRegion.length() - 1;
    }

    private boolean substringMatches(StringBuilder text, String substring, int start) {
        return text.indexOf(substring, start) == start;
    }

    private int seekFirstNonSpaceSymbol(StringBuilder text, int position) {
        while (position < text.length() && Character.isWhitespace(text.charAt(position))) {
            position++;
        }
        return position;
    }

    private int seekLastNonSpaceSymbol(StringBuilder text, int position) {
        while (position > 0 && Character.isWhitespace(text.charAt(position))) {
            position--;
        }
        return position + 1;
    }

}
