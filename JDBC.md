# Tarantool JDBC driver

## 1. Introduction

Java Database Connectivity (JDBC) is a specification that defines an API (set of interfaces) for the programs written in
Java. It defines how a client may access a database. It is part of the Java Standard Edition platform and provides
methods to query and update data in a database.

Since version 2, Tarantool database started supporting relational database features including SQL. Thus, some of the
JDBC features became available to be implemented on the connector side.

Java connector partly implements JDBC 4.2 standard and allows Java programs to connect to a Tarantool 2.x database using
pure Java code. The connector is Type-4 driver written in pure Java, and uses a native network protocol to communicate
with a Tarantool server.

This guide was mainly created to show the basic usage of the driver, highlight and explain some features that may work
in a bit different way than expected or do not work at all because of the Tarantool NoSQL nature or current Tarantool
SQL limitations. By the way, more details and examples can be found inside the JavaDoc API and the JDBC specification.

References:
1. [JDBC Specification](https://download.oracle.com/otndocs/jcp/jdbc-4_2-mrel2-spec/index.html),
1. [JDBC API](https://docs.oracle.com/javase/8/docs/api/java/sql/package-summary.html),
1. [Tarantool SQL Syntax](https://www.tarantool.io/en/doc/2.2/reference/reference_sql/sql/),
1. [Tarantool SQL Binary Protocol](https://www.tarantool.io/en/doc/2.2/dev_guide/internals/sql_protocol/).

## 2. Getting started

### Getting the driver

There are two options how the driver can be obtained. The first way is to download the driver as a project dependency
using following configuration (this is an optimal way if your project uses build tool such as Maven or Gradle):

Maven:

```xml
<dependency>
  <groupId>org.tarantool</groupId>
  <artifactId>connector</artifactId>
  <version>1.9.2</version>
</dependency>
```

Gradle:

```groovy
dependencies {
    compile group: 'org.tarantool', name: 'connector', version: '1.9.2'
}
```

Alternatively, if you need to use a precompiled driver you can download a JAR file from Maven using a direct link (for
instance, [connector-1.9.2.jar](https://repo1.maven.org/maven2/org/tarantool/connector/1.9.2/connector-1.9.2.jar)).

Finally, if you want to make changes to the source code and use a modified version then you should clone [an official
connector repository](https://github.com/tarantool/tarantool-java), apply your patches, and build the driver. To build
the driver it's required a prepared environment as a follows: Java 1.8 or above and Maven 3. Also, the connector source
code is bundled with a Maven wrapper that helps to locally download a proper version of Maven to be used to make a
connector.

Build the project:

```shell script
$ cd tarantool-java
$ mvn clean package
```

or using the provided wrapper

```shell script
$ cd tarantool-java
$ ./mvnw clean package
```

The final JAR file will be placed at `/target` folder according to the Maven conventions.

### Configuring the class path

To start to use the driver in your application the driver JAR file has to be present in java CLASSPATH. It can be
achieved a couple of ways, for instance, setting a CLASSPATH environment variable or using `-cp` (--classpath) option of
`java` command.

Let's set a CLASSPATH variable in context of the running JVM:

```shell script
CLASSPATH=./MyApp.jar:./connector-1.9.2.jar:. java MyApp
```

### Preparing the Tarantool database

Because Java does not support unix sockets as a transport the Tarantool server must be configured to allow TCP/IP
connections. To allow connections you need to configure it properly using a configuration file or execute commands
directly on the database.

Configure the server to listen to 3301 port on localhost:

```lua
box.cfg {
    listen = 3301
}
```

Once you have made sure the server is correctly listening for TCP/IP connections the next step is to verify that users
are allowed to connect to the server.

Add a new user `admin` and grant to it the maximal permissions:

```lua
box.once('bootstrap', function()
    box.schema.user.create('admin', { password = 'p@$$vv0rd' })
    box.schema.user.grant('admin', 'super')
end)
```

References:
1. [Setting the class path by Oracle](https://docs.oracle.com/javase/8/docs/technotes/tools/windows/classpath.html),
1. [Tarantool instance configuration](https://www.tarantool.io/en/doc/2.2/book/admin/instance_config/)
1. [Tarantool database access control](https://www.tarantool.io/en/doc/2.2/book/box/authentication/).

## 3. Initializing the driver

### Using JDBC API

In most cases, in order to use JDBC, an application should import the interfaces from the `java.sql` package:

```java
import java.sql.Connection;
import java.sql.Statement;
```

However, it's possible to import Tarantool JDBC interfaces that extend standard JDBC ones and provide extra incompatible
capabilities. If you want to use those extensions you can add lines like:

```java
import org.tarantool.jdbc.TarantoolConnection;
import org.tarantool.jdbc.TarantoolStatement;
```

**Note**: using vendor specific interfaces decreases portability of your applications. There are good reasons to use
vendor extensions instead of JDBC API.

### Loading the driver

The Tarantool JDBC driver supports the JDBC SPI (Service Provider Interface) that allows to load the driver class
automatically when the JAR file in classpath. The driver class name is `org.tarantool.jdbc.SQLDriver`.

This class can be also loaded manually using:

```java
Class.forName("org.tarantool.jdbc.SQLDriver");
```

or passing the `java.drivers` JVM parameter:

```java
java -Djava.drivers=org.tarantool.jdbc.SQLDriver MyApp
```

### Connecting to the database

The Tarantool driver registers under JDBC sub-protocol called `tarantool`. The driver accepts the following connection
string:

```text
jdbc:tarantool://[user[:password]@][host[:port]][?param1=value1[&param2=value2...]]
```

where

* `user` is an optional username to be used to connect. The default username is `guest`.
* `password` is an optional password defined for a user. The default value is an empty string.
* `host` is an optional hostname of the database. The default value is `localhost`.
* `port` is an optional database port. The default value is `3301`.

To connect, you need to get a `Connection` instance from JDBC. To do this, you use the `DriverManager.getConnection()`
method:

```java
Connection connection = DriverManager.getConnection("jdbc:tarantool//localhost:3301", "admin", "strong-password");
```

**Note**: Tarantool connections aren't pooled and they don't share any resources such as TCP/IP connection. Each
invocation of `DriverManager.getConnection()` returns a new established connection. But, in most cases you don't need to
have several connections in your application because of the internal asynchronous nature of a connection (for more
details see [Understanding the Tarantool connection](#understanding-the-tarantool-connection)).

#### Supported connection parameters

In addition, the driver supports a number of properties which can be used to specify additional driver behaviour
specific to Tarantool. These properties may be specified in either the connection URL or an additional `Properties`
object parameter to `DriverManager.getConnection()`.

* **host** (string) - Tarantool server host. Defaults to `localhost`;
* **port** (integer) - Tarantool server port. Defaults to `3301`;
* **socketChannelProvider** (string) - socket channel provider class that implements
  `org.tarantool.SocketChannelProvider`. Defaults to `org.tarantool.SingleSocketChannelProviderImpl`;
* **user** (string) - username to connect to the database. The default value is `guest`;
* **password** (string) - user's password to connect to the database. The default value is unset;
* **loginTimeout** (integer) - number of milliseconds to wait for a connection establishment. Defaults to 60000 (a
  minute);
* **queryTimeout** (integer) - number of milliseconds to wait before a timeout is occurred for the query. The default
  value is 0 (infinite).

The following examples illustrate the use of the methods to establish a connection.

Using an `Properties` instance:

```java
String url = "jdbc:tarantool://localhost";
Properties props = new Properties();
props.setProperty("user", "admin");
props.setProperty("password", "strong-password");
props.setProperty("port", "3301");
Connection conn = DriverManager.getConnection(url, props);
```

Using the embedded URL string options:

```java
String url = "jdbc:tarantool://admin:strong-password@localhost:3301";
Connection conn = DriverManager.getConnection(url);
```

Or using URL query parameters:

```java
String url = "jdbc:postgresql://localhost?port=3301&user=admin&password=strong-password";
Connection conn = DriverManager.getConnection(url);
```

If a property is specified both in URL main part (before `?` sign) and in URL query part (after `?` sign), the value
from main part is ignored.

If a property is specified both in URL and in `Properties` object, the value from `Properties` takes precedence.

### Using the driver in the enterprise environment

The enterprise environment (i.e. JavaEE) often uses `javax.sql` package to configure and manage connections, distributed
transactions and so on. The Tarantool driver provides an implementation of `DataSource`
(`org.tarantool.jdbc.ds.SQLDataSource`) to be more compatible with JDBC specification.

Usually, JDBC drivers implement connection pooling under the `DataSource` abstraction (including
`ConnectionPoolDataSource` and `PooledConnection`) to increase performance of applications. However, the Tarantool
driver does not do pooling because of using an asynchronous approach to send the request that allows to reuse the same
connection by different threads (see [Understanding the Tarantool connection](#understanding-the-tarantool-connection)).

For example, a data source can be instantiated directly:

```java
SQLDataSource source = new SQLDataSource();
source.setDataSourceName("Tarantool Datasource");
source.setServerName("localhost");
source.setPortNumber(3301);
source.setUser("admin");
source.setPassword("string-password");

Connection connection = source.getConnection();
```

#### Supported `DataSource` properties

Most of the datasource properties repeat the respective connection parameters (see
[Supported connection parameters](#supported-connection-parameters)).

* **serverName** (string) - Tarantool server host. Defaults to `localhost`;
* **portNumber** (integer) - Tarantool server port. Defaults to `3301`;
* **socketChannelProvider** (string) - socket channel provider class that implements
  `org.tarantool.SocketChannelProvider`. Defaults to `org.tarantool.SingleSocketChannelProviderImpl`;
* **user** (string) - username to connect to the database. The default value is `guest`;
* **password** (string) - user's password to connect to the database. The default value is unset;
* **description** (read-only, string) - vendor specific definition of the data source;
* **datasourceName** (string) - data source name;
* **loginTimeout** (integer) - number of milliseconds to wait for a connection establishment. Defaults to 60000 (a
  minute);
* **queryTimeout** (integer) - number of milliseconds to wait before a timeout is occurred for the query. The default
  value is 0 (infinite).

### Closing the connection

It usually should be avoided to close the connection manually because of it is done by your application container. But
if you want to shutdown the connection you need to use `Connection.close()` method. Once you close the connection all
the associated resources such as statements and result sets will be automatically released. Most of the their methods
will became unable to invoke anymore.

### Understanding the Tarantool connection

The implementation of Tarantool JDBC connection uses asynchronous requests under the hood via
`org.tarantool.TarantoolCliemtImpl#asyncOps()`. It makes possible to issue queries in a multi-threaded environment and
do not block each other on awaiting responses using just one connection instance. There is no positive impaction on
performance using multiple connections because of driver's capability to send different requests simultaneously using a
single physical connection to the database. On the other hand, it may cause reordering of the request and their
responses, even if they are executed in the same thread serially.

All connections don't support transactions currently. Thus, each connection is auto-committed and it cannot be
disabled. Also, lack of the transaction support impacts on connection isolation level which is always
`Connection.TRANSACTION_NONE` and the result set holdability where the `ResultSet.HOLD_CURSORS_OVER_COMMIT` is the only
 mode supported.

The callable statements (`CallableStatement` objects) are also unsupported now. It requires an appropriate feature on
the server side to be implemented.

## 3. Working with the database

### Issuing a query

To issue SQL statements to the database, you require a `Statement` or `PreparedStatement` instance. Once you have the
statement, you can use it to send a query or update. This will return a `ResultSet` object, which contains the entire
result.

Let's fetch all rich employees at a company:

```java
try (
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT e.email FROM employee e WHERE e.salary > 1000");
) {
    List<String> emails = new ArrayList<>();
    while (resultSet.next()) {
        emails.add(resultSet.getString(1));
    }
    sendMail(emails, "Heya, you are fired!");
}
```

or do the same using `PreparedStatement`:

```java
PreparedStatement statement = connection.prepareStatement("SELECT e.email FROM employee e WHERE e.salary > ?");
statement.setInt(1, 1000);

ResultSet resultSet = statement.executeQuery();

List<String> emails = new ArrayList<>();
while (resultSet.next()) {
    emails.add(resultSet.getString(1));
}
sendMail(emails, "Heya, you are fired!");

resultSet.close();
statement.close();
```

Primarily, the `Statement` is used for simple queries that do not contain parameter placeholders (marked by `?` sign).
Also, there are `PreparedStatement` objects that support input parameters to be placed instead of the placeholders. And
the third statement type named `CallableStatement` is unsupported at the moment.

JDBC provides two kinds of results that can be received using statements; the first type describes DQL query result and
through the `ResultSet` objects and second - DML/DDL query result as a single number of rows affected.

The statements support the following properties related to the results handling:

* `Statement.CLOSE_CURRENT_RESULT` mode. In general it is unsupported to obtain multiple result sets per execution using
  `Statement.getMoreResults()` method. However it can be invoked and close the current open result according to the API.

* `Statement.NO_GENERATED_KEYS` and `Statement.RETURN_GENERATED_KEYS` options. The first option works by default and
  it is usually used when you do not want to receive the generated values. The second option relies on the support
  on the server side and allow to receive only _auto-generated_ values for the primary keys. The generated IDs can be
  received via `Statement.getGeneratedKeys()` that returns a result set with a single integer column named
  `GENERATED_KEY`.

* Limiting the rows returned using `Statement.setMaxRows(int)` does not use protocol specific features to reduce the
  result set and does it in the client memory.

**Note**: there are no such differences between `Statement` and `PreparedStatement` as they can be. Ideally, the
prepared statements could be pre-compiled and cached (i.e. using _server prepared statements_). Another issue here is
the not properly working `PreparedStatement.getMetaData()` method that allows to fetch result set metadata before
it will be executed.

### Processing a result

An example that shows how the `ResultSet` can be obtained:

```java
ResultSet resultSet = statement.executeQuery("SELECT * FROM animal a WHERE a.class = 'mammal");
```

Next, the obtained `ResultSet` can be used to traverses a set using group of appropriate methods (`next()`,
`previous()`, `first()`, `last()` and others). And columns can be received via a rich family of getter methods.

```java
resultSet.afterLast();
while (resultSet.previous()) {
    processRow(resultSet.getString(1), resultSet.getInt(2));
}
```

The statements support producing `ResultSet` with the characteristics as follows:

* `ResultSet.TYPE_FORWARD_ONLY` and `ResultSet.TYPE_SCROLL_INSENSITIVE` scroll types. The driver does not support
  cursors and keeps all rows in memory. It allows to traverse through the rows using bi-directional, relative, and
  absolute movements. The forward only mode does not make sense here a lot but is supported as a mandatory JDBC feature.

* `ResultSet.CONCUR_READ_ONLY` concurrency mode. All result sets created by statements are immutable and it is
impossible to use `ResultSet.update*()` methods to modify the data and reflect the changes back to the database.

* `ResultSet.HOLD_CURSORS_OVER_COMMIT` holdability type. The transactions are not supported yet. It means each statement
  execution is auto-committed and keeping the result open is the only way to deal with it.

* `ResultSet.FETCH_FORWARD` hint is the only supported value now. Other hints are ignored by the driver.

#### Closing a result set

It is a best practice to use `AutoClosable` interface and the _try-with-resources_ block. It allows to close
unwanted instances and release their resources acquired.

```java
try (
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT e.* FROM employee e");
) {
    // process the result
}
```

#### Getting auto-generated values

It's possible to receive back the values being generated by server side and unknown before the execution. Tarantool DB
supports such values to be returned for _auto-increment_ primary keys.

Consider a simple example how to get the generated values:

```java
// CREATE TABLE tag (id INT PRIMARY KEY AUTOINCREMENT, tag_name TEXT);

statement.executeUpdate("INSERT INTO tag VALUES (null, 'photo'), (null, 'music')", Statement.RETURN_GENERATED_KEYS);
int rows = statement.getUpdateCount();
if (rows > 0) {
    List<Integer> ids = new ArrayList<>();
    try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
        while (generatedKeys.next()) {
            int newId = generatedKeys.getInt("GENERATED_KEY");
            ids.add(newId);
        }
    }
}
statement.close();
processNewTags(ids);
```

**Note**: the `Statement.getGeneratedKeys()` should be called once. The received `ResultSet` object will be the same
if `getGeneratedKeys()` is invoked multiple times. Once this object is closed it cannot be obtain again using
`getGeneratedKeys()`.

### Applying changes

There is a way to perform modifications and get a count of affected rows using statements.

Typical case can be illustrated as follows:

```java
PreparedStatement statement = connection.prepareStatement("UPDATE animal SET in_redlist = ? WHERE name = ?");
statement.setBoolean(1, true);
statement.setString(2, 'Spoon-billed sandpiper');

int rows = statement.executeUpdate();
```

#### Batching updates

The `Statement` and `PreparedStatement` objects can be used to submit batch updates.

For instance, using `Statement` you can send a bundle of heterogeneous statements:

```java
Statement statement = connection.createStatement();
statement.addBatch("INSERT INTO student VALUES (30, 'Joe Jones')");
statement.addBatch("INSERT INTO faculty VALUES (2, 'Faculty of Chemistry')");
statement.addBatch("INSERT INTO student_faculty VALUES (30, 2)");

int[] updateCounts = statement.executeBatch();
```

Using `PreparedStatement` you can reuse bindings of the same statement several times and send a set of the input
parameters as a single unit:

```java
PreparedStatement statement = connection.prepareStatement("INSERT INTO student VALUES (?, ?)");

statement.setInt(1, 30);
statement.setString(2, "Michael Korj");
statement.addBatch();

statement.setInt(1, 40);
statement.setString(2, "Linda Simpson");
statement.addBatch();

int[] updateCounts = statement.executeBatch();
```

The driver uses a pipelining when it performs a batch request. It means each query is asynchronously sent one-by-one in
order they were specified in the batch.

**Note**: JDBC spec recommends that *auto-commit* mode should be disabled to prevent the driver from committing a
transaction when a batch request is called. The driver is not support transactions and *auto-commit* is always enabled,
so each statement from the batch is executed in its own transaction. In particular, it causes the driver to continue
processing the remaining commands in a batch if the execution of a command fails.

**Note**: There are some case when an order of batch requests can be corrupted. The first case is that DDL requests are
not transactional in Tarantool. Thus, a batch containing such operations can produce an undefined behaviour:

```java
// second statement can fail with an error that `student` table does not exist

statement.addBatch("CREATE TABLE student (id INT PRIMARY KEY, name VARCHAR(100))");
statement.addBatch("INSERT INTO student VALUES (1, 'Alex Smith')");
```

Moreover, if `vinyl` storage engine is used an execution order of batch statements is also not specified. This behaviour
is incompatible with JDBC spec in the sentence "Batch commands are executed serially (at least logically) in the order
in which they were added to the batch".

### Closing a statement

The best option here is to use statements with _try-with-resources_ construction that allow to release all held
resources just after a statement object is not needed anymore. The closing a statement also closes all result sets
created by the statement including generated keys result set.

There is an optimal structure of a local statement usage

```java
try (Statement statement = connection.createStatement()) {
    // deal with the statement
}
```

### Using escape syntax [in development]

JDBC allows to use a special vendor-free syntax called _escape syntax_. Escape syntax is designed to be easily scanned
and parsed by the driver and processed in the vendor-specific way. Implementing this special processing in the driver
layer improves application portability.

Escape processing for a `Statement` object is turned on or off using the method `setEscapeProcessing(bool)`, with the
default being on.

**Note**: the `PreparedStatement.setEscapeProcessing()` method does not work because its SQL string may have been
precompiled when the `PreparedStatement` object was created.

`Connection.nativeSQL(String)` provides another way to have escapes processed. It translates the given SQL to a SQL
suitable for the Tarantool database.

JDBC defines escape syntax for the following:

* scalar functions (partially supported)
* date and time literals (not supported)
* outer joins (supported)
* calling stored procedures and functions (not supported)
* escape characters for LIKE clauses (supported)
* returned rows limit (supported)

Let's consider the following case of using escapes:

```java
Statement statement = connection.createStatement();
ResultSet resultSet = statement.executeQuery(
    "SELECT {fn concat(e.name, m.name)} FROM {oj employee e LEFT OUTER JOIN employee m ON e.manager_id = m.id} {limit 5}"
);
```

which will be translated to less portable version:

```java
Statement statement = connection.createStatement();
ResultSet resultSet = statement.executeQuery(
    "SELECT (e.name || m.name) FROM employee e LEFT OUTER JOIN employee m ON e.manager_id = m.id limit 5"
);
```

**Note**: the driver uses quite simple rules processing escape syntax where the most of the supported expressions
translates directly by omitting escape boundaries `{}`. Thus, the `{fn abs(-5)}}` becomes `abs(-5)` or
`{limit 10 offset 50}` becomes `limit 10 offset 50` and so on. It may lead generating an incorrect SQL statement if you
use bad escape expressions such as `{limit 'one' offset 'zero'}` or `{escape 45}` etc. The escape processing does not
include a full parsing of SQL statements to be sure they are valid (for instance, `SELECT {limit 1} * FROM table`).

#### Scalar Functions

The escape syntax to access a scalar function is: `{fn function_name(arguments)}`.

The following tables show which functions are supported by the driver. The driver supports the nesting and the mixing of
escaped functions and escaped values.

**Supported numeric scalar functions:**

JDBC escape           | Native                | Comment
--------------------- | --------------------- | ----------------------------------------------------------------
ABS(number)           | ABS(number)           |
PI()                  | 3.141592653589793     | Driver replaces the function by the `java.lang.Math.PI` constant
RAND(seed)            | RANDOM()              | The `seed` param is ignored
ROUND(number, places) | ROUND(number, places) |

**Supported string scalar functions:**

JDBC escape                                  | Native                                     | Comment
-------------------------------------------- | ------------------------------------------ | -----------------------------------------
CHAR(code)                                   | CHAR(code)                                 |
CHAR_LENGTH(code, CHARACTERS])               | CHAR_LENGTH(code)                          | Last optional parameter is not supported
CHARACTER_LENGTH(code, CHARACTERS])          | CHARACTER_LENGTH(code)                     | Last optional parameter is not supported
CONCAT(string1, string2)                     | (string1 &#124;&#124; string2)             |
LCASE(string)                                | LOWER(string)                              |
LEFT(string, count)                          | SUBSTR(string, 1, count)                   |
LENGTH(string, CHARACTERS)                   | LENGTH(TRIM(TRAILING FROM string))         | Last optional parameters is not supported
LTRIM(string)                                | TRIM(LEADING FROM string)                  |
REPLACE(string1, string2, string3)           | REPLACE(string1, string2, string3)         |
RIGHT(string, count)                         | SUBSTR(string, LENGTH(string) - count + 1) | Can produce too long native expression because of the string is used twice
RTRIM(string)                                | TRIM(TRAILING FROM string)                 |
SOUNDEX(string)                              | SOUNDEX(string)                            |
SUBSTRING(string, start, length, CHARACTERS) | SUBSTR(string, start, length)              | Last optional parameters is not supported
UCASE(string)                                | UPPER(string)                              |

**Supported system scalar functions:**

JDBC escape                      | Native                           | Comment
-------------------------------- | -------------------------------- | ----------------------------------------------------------------
DATABASE()                       | 'DEFAULT'                        | Tarantool does not support databases. Driver always replaces it by 'DEFAULT'
IFNULL(expression1, expression2) | IFNULL(expression1, expression2) |
USER()                           | <string literal>                 | Driver replaces the function to the current user name

Let's take a look at a few examples of using scalar functions:

```java
Statement statement = connection.createStatement();
// SELECT 'DEFAULT'
ResultSet resultSet = statement.executeQuery("SELECT {fn database()}");
```

```java
Statement statement = connection.createStatement();
// SELECT UPPER('usa')
ResultSet resultSet = statement.executeQuery("SELECT {fn ucase('usa')}");
```

```java
Statement statement = connection.createStatement();
// SELECT 2 * 3.141592653589793 * 3.141592653589793 / ABS(RANDOM() - ROUND(3.141592653589793, 4))
ResultSet resultSet = statement.executeQuery(
    "SELECT 2 * {fn pi()} * {fn pi()} / {fn abs({fn rand(252)} - {fn round({fn pi()}, 4)})}"
);
```

#### Outer joins

The escape syntax for an outer join is: `{oj <outer-join>}` where `<outer-join>` has the form:
` table {LEFT|RIGHT|FULL} OUTER JOIN {table | <outer-join>} ON search-condition`

The following SELECT statement uses the escape syntax for an outer join:

```java
Statement statement = connection.createStatement();
// SELECT * FROM employee e LEFT OUTER JOIN department d ON e.dept_id = d.id
ResultSet resultSet = statement.executeQuery(
    "SELECT * FROM {oj employee e LEFT OUTER JOIN department d ON e.dept_id = d.id}"
);
```

**Note**: even though the driver allows to use any available SQL join operators the Tarantool itself supports only the
following variants: [NATURAL] LEFT [OUTER] JOIN, [NATURAL] INNER JOIN, or CROSS JOIN.

#### `LIKE` escape characters

The percent sign `%` and underscore `_` characters are wild card characters in SQL LIKE clauses. To use them as
literal symbols, they can be preceded by a backslash `\`, which is a special escape character in strings. You can
specify which character to use as the escape character by including the following syntax at the end of a LIKE predicate:
`{escape '<escape-character>'}`.

For example, let's compare string values using '|' as an escape character to protect '%' character:

```java
Statement statement = connection.createStatement();
// SELECT * FROM item WHERE description LIKE '|%type' escape '|'
ResultSet resultSet = statement.executeQuery("SELECT * FROM item WHERE description LIKE '|%type' {escape '|'}");
```

#### Limiting returned rows escape

The escape syntax for limiting the number of rows returned by a query is: `{limit rows [offset row_offset]}`.

The value given for `rows` indicates the maximum number of rows to be returned from this query. The `row_offset`
indicates the number of rows to skip from the rows returned from the query before beginning to return rows.

The following query will return no more than 10 rows:

```java
Statement statement = connection.createStatement();
// SELECT * FROM student LIMIT 10
ResultSet resultSet = statement.executeQuery("SELECT * FROM table {limit 10}");
```
