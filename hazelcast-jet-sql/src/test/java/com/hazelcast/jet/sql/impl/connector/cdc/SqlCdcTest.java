package com.hazelcast.jet.sql.impl.connector.cdc;

import com.hazelcast.jet.sql.SqlTestSupport;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.MySQLContainer;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

public class SqlCdcTest extends SqlTestSupport {

    private static final String USER = "debezium";
    private static final String PASSWORD = "dbz";
    private static final String SERVER_NAME = "dbserver1";

    private static final String SCHEMA_NAME = "inventory";
    private static final String TABLE_NAME = "customers";

    private static final String MY_SQL_CONNECTOR_CLASS_NAME = "io.debezium.connector.mysql.MySqlConnector";

    @ClassRule
    public static MySQLContainer<?> mysql = new MySQLContainer<>("debezium/example-mysql:1.2")
            .withUsername("mysqluser")
            .withPassword("mysqlpw");

    @BeforeClass
    public static void beforeClass() {
        executeSql(format("CREATE EXTERNAL TABLE %s (id INT, first_name VARCHAR, last_name VARCHAR, email VARCHAR) " +
                        "TYPE \"%s\" " +
                        "OPTIONS (" +
                        "  \"connector.class\" '%s', " +
                        "  \"database.hostname\" '%s', " +
                        "  \"database.port\" '%s', " +
                        "  \"database.user\" '%s', " +
                        "  \"database.password\" '%s', " +
                        "  \"database.server.id\" '1', " +
                        "  \"database.server.name\" '%s', " +
                        "  \"database.whitelist\" '%s', " +
                        "  \"table.whitelist\" '%s' " +
                        ")",
                TABLE_NAME, CdcSqlConnector.TYPE_NAME, MY_SQL_CONNECTOR_CLASS_NAME,
                mysql.getContainerIpAddress(), mysql.getMappedPort(MYSQL_PORT), USER, PASSWORD, SERVER_NAME,
                SCHEMA_NAME, SCHEMA_NAME + "." + TABLE_NAME
        ));
    }

    @Test
    public void select_unicodeConstant() {
        assertRowsEventuallyAnyOrder(
                format("SELECT '喷气式飞机' FROM %s", TABLE_NAME),
                asList(
                        new Row("喷气式飞机"),
                        new Row("喷气式飞机"),
                        new Row("喷气式飞机"),
                        new Row("喷气式飞机")));
    }

    @Test
    public void fullScan() {
        assertRowsEventuallyAnyOrder(
                format("SELECT id, last_name, first_name, email  FROM %s", TABLE_NAME),
                asList(
                        new Row(1001, "Thomas", "Sally", "sally.thomas@acme.com"),
                        new Row(1002, "Bailey", "George", "gbailey@foobar.com"),
                        new Row(1003, "Walker", "Edward", "ed@walker.com"),
                        new Row(1004, "Kretchmar", "Anne", "annek@noanswer.org")));
    }

    @Test
    public void fullScan_star() {
        assertRowsEventuallyAnyOrder(
                format("SELECT * FROM %s", TABLE_NAME),
                asList(
                        new Row(1001, "Sally", "Thomas", "sally.thomas@acme.com"),
                        new Row(1002, "George", "Bailey", "gbailey@foobar.com"),
                        new Row(1003, "Edward", "Walker", "ed@walker.com"),
                        new Row(1004, "Anne", "Kretchmar", "annek@noanswer.org")));
    }

    @Test
    public void fullScan_filter() {
        assertRowsEventuallyAnyOrder(
                format("SELECT * FROM %s WHERE id=1002 or first_name='Anne'", TABLE_NAME),
                asList(
                        new Row(1002, "George", "Bailey", "gbailey@foobar.com"),
                        new Row(1004, "Anne", "Kretchmar", "annek@noanswer.org")));
    }

    @Test
    public void fullScan_projection1() {
        assertRowsEventuallyAnyOrder(
                format("SELECT upper(first_name) FROM %s WHERE id='1001'", TABLE_NAME),
                singletonList(new Row("SALLY")));
    }

    @Test
    public void fullScan_projection2() {
        assertRowsEventuallyAnyOrder(
                format("SELECT id FROM %s WHERE upper(last_name)='BAILEY'", TABLE_NAME),
                singletonList(new Row(1002)));
    }

    @Test
    public void fullScan_projection3() {
        assertRowsEventuallyAnyOrder(
                format("SELECT last_name FROM (SELECT upper(last_name) last_name FROM %s) WHERE last_name='WALKER'", TABLE_NAME),
                singletonList(new Row("WALKER")));
    }

    @Test
    public void fullScan_projection4() {
        assertRowsEventuallyAnyOrder(
                format("SELECT upper(email) FROM %s WHERE upper(email)='ED@WALKER.COM'", TABLE_NAME),
                singletonList(new Row("ED@WALKER.COM")));
    }
}
