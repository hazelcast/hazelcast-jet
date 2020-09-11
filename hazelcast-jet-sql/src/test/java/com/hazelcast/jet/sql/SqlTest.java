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

package com.hazelcast.jet.sql;

import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class SqlTest extends JetSqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_values() {
        assertEmpty(
                "SELECT a - b FROM (VALUES (1, 2)) AS t (a, b) WHERE a + b > 4"
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT a - b FROM (VALUES (7, 11)) AS t (a, b) WHERE a + b > 4",
                singletonList(new Row((byte) -4))
        );

        assertRowsEventuallyInAnyOrder("SELECT * FROM (VALUES('a'))",
                singletonList(new Row("a")));
    }

    @Test
    public void test_multipleValues_select() {
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM (VALUES (1), (2))",
                asList(new Row((byte) 1), new Row((byte) 2))
        );
    }

    @Test
    public void test_multipleValues_insert() {
        sqlService.execute(javaSerializableMapDdl("m", Integer.class, Integer.class));
        sqlService.execute("INSERT OVERWRITE m(__key, this) VALUES (1, 1), (2, 2)");

        IMap<Integer, Integer> map = instance().getMap("m");
        assertEquals(2, map.size());
        assertEquals(1, (int) map.get(1));
        assertEquals(2, (int) map.get(2));
    }

    @Test
    public void test_unicodeConstant() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsEventuallyInAnyOrder(
                "SELECT '喷气式飞机' FROM t",
                asList(
                        new Row("喷气式飞机"),
                        new Row("喷气式飞机")));
    }

    @Test
    public void test_fullScan() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsEventuallyInAnyOrder(
                "SELECT v FROM t",
                asList(
                        new Row(0),
                        new Row(1)));
    }

    @Test
    public void test_fullScanStar() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM t",
                asList(
                        new Row(0),
                        new Row(1)));
    }

    @Test
    public void test_fullScanFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsEventuallyInAnyOrder(
                "SELECT v FROM t WHERE v=0 OR v=1",
                asList(
                        new Row(0),
                        new Row(1)));
    }

    @Test
    public void test_fullScanProjection1() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsEventuallyInAnyOrder(
                "SELECT v + v FROM t WHERE v=1",
                singletonList(new Row(2L)));
    }

    @Test
    public void test_fullScanProjection2() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsEventuallyInAnyOrder(
                "SELECT v FROM t WHERE v+v=2",
                singletonList(new Row(1)));
    }

    @Test
    public void fullScan_projection3() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsEventuallyInAnyOrder(
                "SELECT v2 FROM (SELECT v+v v2 FROM t) WHERE v2=2",
                singletonList(new Row(2L)));
    }

    @Test
    public void test_fullScanProjection4() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsEventuallyInAnyOrder(
                "SELECT v+v FROM t WHERE v+v=2",
                singletonList(new Row(2L)));
    }
}
