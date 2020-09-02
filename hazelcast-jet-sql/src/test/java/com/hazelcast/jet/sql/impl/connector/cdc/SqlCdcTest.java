/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.cdc;

import com.hazelcast.jet.sql.JetSqlTestSupport;
import com.hazelcast.sql.SqlService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.MySQLContainer;

import static com.hazelcast.jet.sql.impl.connector.cdc.CdcSqlConnector.OPERATION;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

public class SqlCdcTest extends JetSqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getHazelcastInstance().getSql();
    }

    @ClassRule
    public static MySQLContainer<?> mysql = new MySQLContainer<>("debezium/example-mysql:1.2")
            .withUsername("mysqluser")
            .withPassword("mysqlpw");

    private static final String USER = "debezium";
    private static final String PASSWORD = "dbz";
    private static final String SERVER_NAME = "dbserver1";

    private static final String SCHEMA_NAME = "inventory";
    private static final String TABLE_NAME = "customers";

    private static final String MY_SQL_CONNECTOR_CLASS_NAME = "io.debezium.connector.mysql.MySqlConnector";

    @Before
    public void before() {
        sqlService.execute("CREATE EXTERNAL TABLE " + TABLE_NAME + " ( "
                + OPERATION + " VARCHAR, "
                + "id INT, "
                + "first_name VARCHAR, "
                + "last_name VARCHAR, "
                + "email VARCHAR) "
                + "TYPE \"" + CdcSqlConnector.TYPE_NAME + "\" "
                + "OPTIONS ( "
                + "\"connector.class\" '" + MY_SQL_CONNECTOR_CLASS_NAME + "', "
                + "\"database.hostname\" '" + mysql.getContainerIpAddress() + "', "
                + "\"database.port\" '" + mysql.getMappedPort(MYSQL_PORT) + "', "
                + "\"database.user\" '" + USER + "', "
                + "\"database.password\" '" + PASSWORD + "', "
                + "\"database.server.id\" '1', "
                + "\"database.server.name\" '" + SERVER_NAME + "', "
                + "\"database.whitelist\" '" + SCHEMA_NAME + "', "
                + "\"table.whitelist\" '" + SCHEMA_NAME + "." + TABLE_NAME
                + "')");
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
                format("SELECT id, last_name, first_name, email FROM %s", TABLE_NAME),
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
                        new Row("c", 1001, "Sally", "Thomas", "sally.thomas@acme.com"),
                        new Row("c", 1002, "George", "Bailey", "gbailey@foobar.com"),
                        new Row("c", 1003, "Edward", "Walker", "ed@walker.com"),
                        new Row("c", 1004, "Anne", "Kretchmar", "annek@noanswer.org")));
    }

    @Test
    public void fullScan_filter() {
        assertRowsEventuallyAnyOrder(
                format("SELECT * FROM %s WHERE id=1002 or first_name='Anne'", TABLE_NAME),
                asList(
                        new Row("c", 1002, "George", "Bailey", "gbailey@foobar.com"),
                        new Row("c", 1004, "Anne", "Kretchmar", "annek@noanswer.org")));
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
                format("SELECT last_name FROM (SELECT upper(last_name) last_name FROM %s) WHERE last_name='WALKER'",
                        TABLE_NAME),
                singletonList(new Row("WALKER")));
    }

    @Test
    public void fullScan_projection4() {
        assertRowsEventuallyAnyOrder(
                format("SELECT upper(email) FROM %s WHERE upper(email)='ED@WALKER.COM'", TABLE_NAME),
                singletonList(new Row("ED@WALKER.COM")));
    }
}
