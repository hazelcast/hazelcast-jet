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

import com.hazelcast.jet.sql.impl.connector.test.AllTypesSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
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
        sqlService.execute("SINK INTO m(__key, this) VALUES (1, 1), (2, 2)");

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
                        new Row("喷气式飞机")
                )
        );
    }

    @Test
    public void test_fullScan() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsEventuallyInAnyOrder(
                "SELECT v FROM t",
                asList(
                        new Row(0),
                        new Row(1)
                )
        );
    }

    @Test
    public void test_fullScanStar() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM t",
                asList(
                        new Row(0),
                        new Row(1)
                )
        );
    }

    @Test
    public void test_fullScanFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsEventuallyInAnyOrder(
                "SELECT v FROM t WHERE v=0 OR v=1",
                asList(
                        new Row(0),
                        new Row(1)
                )
        );
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
                singletonList(new Row(1))
        );
    }

    @Test
    public void fullScan_projection3() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsEventuallyInAnyOrder(
                "SELECT v2 FROM (SELECT v+v v2 FROM t) WHERE v2=2",
                singletonList(new Row(2L))
        );
    }

    @Test
    public void test_fullScanProjection4() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsEventuallyInAnyOrder(
                "SELECT v+v FROM t WHERE v+v=2",
                singletonList(new Row(2L))
        );
    }

    @Test
    public void test_queryMetadata() {
        AllTypesSqlConnector.create(sqlService, "t");

        SqlResult result = sqlService.execute("SELECT * FROM t");

        assertThat(result.isUpdateCount()).isFalse();
        assertThat(result.getRowMetadata().getColumnCount()).isEqualTo(13);
        assertThat(result.getRowMetadata().getColumn(0).getName()).isEqualTo("string");
        assertThat(result.getRowMetadata().getColumn(0).getType()).isEqualTo(SqlColumnType.VARCHAR);
        assertThat(result.getRowMetadata().getColumn(1).getName()).isEqualTo("boolean");
        assertThat(result.getRowMetadata().getColumn(1).getType()).isEqualTo(SqlColumnType.BOOLEAN);
        assertThat(result.getRowMetadata().getColumn(2).getName()).isEqualTo("byte");
        assertThat(result.getRowMetadata().getColumn(2).getType()).isEqualTo(SqlColumnType.TINYINT);
        assertThat(result.getRowMetadata().getColumn(3).getName()).isEqualTo("short");
        assertThat(result.getRowMetadata().getColumn(3).getType()).isEqualTo(SqlColumnType.SMALLINT);
        assertThat(result.getRowMetadata().getColumn(4).getName()).isEqualTo("int");
        assertThat(result.getRowMetadata().getColumn(4).getType()).isEqualTo(SqlColumnType.INTEGER);
        assertThat(result.getRowMetadata().getColumn(5).getName()).isEqualTo("long");
        assertThat(result.getRowMetadata().getColumn(5).getType()).isEqualTo(SqlColumnType.BIGINT);
        assertThat(result.getRowMetadata().getColumn(6).getName()).isEqualTo("float");
        assertThat(result.getRowMetadata().getColumn(6).getType()).isEqualTo(SqlColumnType.REAL);
        assertThat(result.getRowMetadata().getColumn(7).getName()).isEqualTo("double");
        assertThat(result.getRowMetadata().getColumn(7).getType()).isEqualTo(SqlColumnType.DOUBLE);
        assertThat(result.getRowMetadata().getColumn(8).getName()).isEqualTo("decimal");
        assertThat(result.getRowMetadata().getColumn(8).getType()).isEqualTo(SqlColumnType.DECIMAL);
        assertThat(result.getRowMetadata().getColumn(9).getName()).isEqualTo("time");
        assertThat(result.getRowMetadata().getColumn(9).getType()).isEqualTo(SqlColumnType.TIME);
        assertThat(result.getRowMetadata().getColumn(10).getName()).isEqualTo("date");
        assertThat(result.getRowMetadata().getColumn(10).getType()).isEqualTo(SqlColumnType.DATE);
        assertThat(result.getRowMetadata().getColumn(11).getName()).isEqualTo("timestamp");
        assertThat(result.getRowMetadata().getColumn(11).getType()).isEqualTo(SqlColumnType.TIMESTAMP);
        assertThat(result.getRowMetadata().getColumn(12).getName()).isEqualTo("timestampTz");
        assertThat(result.getRowMetadata().getColumn(12).getType()).isEqualTo(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE);
    }

    @Test
    public void test_sinkMetadata() {
        sqlService.execute(javaSerializableMapDdl("m", Integer.class, Integer.class));

        SqlResult result = sqlService.execute("SINK INTO m(__key, this) VALUES (1, 1), (2, 2)");

        assertThat(result.isUpdateCount()).isTrue();
        assertThat(result.updateCount()).isEqualTo(-1);
    }
}
