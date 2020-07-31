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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.jet.sql.SqlTestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.sql.SqlConnector.AVRO_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.SqlConnector.TO_SERIALIZATION_FORMAT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class SqlFileTest extends SqlTestSupport {

    private static String name;

    @BeforeClass
    public static void beforeClass() {
        name = createRandomName();
        executeSql("CREATE EXTERNAL TABLE " + name + " ("
                + "username VARCHAR"
                + ", age INT"
                + ") TYPE \"" + FileSqlConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + FileSqlConnector.TO_DIRECTORY + "\" '" + RESOURCES_PATH + "'"
                + ", \"" + FileSqlConnector.TO_GLOB + "\" '" + "users.avro" + "'"
                + ", \"" + TO_SERIALIZATION_FORMAT + "\" '" + AVRO_SERIALIZATION_FORMAT + "'"
                + ")"
        );
    }

    @Test
    public void select_unicodeConstant() {
        assertRowsEventuallyAnyOrder(
                "SELECT '喷气式飞机' FROM " + name,
                asList(
                        new Row("喷气式飞机"),
                        new Row("喷气式飞机")
                )
        );
    }

    @Test
    public void fullScan() {
        assertRowsEventuallyAnyOrder(
                "SELECT age, username FROM " + name,
                asList(
                        new Row(0, "User0"),
                        new Row(1, "User1")
                )
        );
    }

    @Test
    public void fullScan_star() {
        assertRowsEventuallyAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row("User0", 0),
                        new Row("User1", 1)
                )
        );
    }

    @Test
    public void fullScan_filter1() {
        assertRowsEventuallyAnyOrder(
                "SELECT username FROM " + name + " WHERE age=1",
                singletonList(new Row("User1"))
        );
    }

    @Test
    public void fullScan_filter2() {
        assertRowsEventuallyAnyOrder(
                "SELECT username FROM " + name + " WHERE age=1 or username='User0'",
                asList(
                        new Row("User0"),
                        new Row("User1")
                )
        );
    }

    @Test
    public void fullScan_projection1() {
        assertRowsEventuallyAnyOrder(
                "SELECT upper(username) FROM " + name + " WHERE age=1",
                singletonList(new Row("USER1"))
        );
    }

    @Test
    public void fullScan_projection2() {
        assertRowsEventuallyAnyOrder(
                "SELECT username FROM " + name + " WHERE upper(username)='USER1'",
                singletonList(new Row("User1"))
        );
    }

    @Test
    public void fullScan_projection3() {
        assertRowsEventuallyAnyOrder(
                "SELECT name FROM (SELECT upper(username) name FROM " + name + ") WHERE name='USER1'",
                singletonList(new Row("USER1"))
        );
    }

    @Test
    public void fullScan_projection4() {
        assertRowsEventuallyAnyOrder(
                "SELECT upper(username) FROM " + name + " WHERE upper(username)='USER1'",
                singletonList(new Row("USER1"))
        );
    }

    @Test
    public void file_tableFunction() {
        assertRowsEventuallyAnyOrder(
                "SELECT username, age FROM TABLE (" +
                        "FILE (format => 'avro', directory => '" + RESOURCES_PATH + "', glob => 'users.avro')" +
                        ")",
                asList(
                        new Row("User0", 0),
                        new Row("User1", 1)
                )
        );
    }

    private static String createRandomName() {
        return "file_" + randomString().replace('-', '_');
    }
}
