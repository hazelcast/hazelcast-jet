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
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class SqlAggregateTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_group() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{null, "1"},
                new String[]{"Alice", "1"},
                new String[]{null, "1"}
        );

        assertRowsAnyOrder(
                "SELECT name FROM " + name + " GROUP BY 1", // field ordinal
                asList(
                        new Row("Alice"),
                        new Row("Bob"),
                        new Row((String) null)
                )
        );
        assertRowsAnyOrder(
                "SELECT distance, name FROM " + name + " GROUP BY distance, name",
                asList(
                        new Row(1, "Alice"),
                        new Row(2, "Alice"),
                        new Row(1, "Bob"),
                        new Row(1, null)
                )
        );
    }

    @Test
    public void test_groupExpression() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, (distance * 2) d FROM " + name + " GROUP BY name, d",
                asList(
                        new Row("Alice", 2L),
                        new Row("Alice", 4L),
                        new Row("Bob", 2L)
                )
        );
    }

    @Test
    public void test_groupHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, distance FROM " + name + " GROUP BY name, distance HAVING distance < 2",
                asList(
                        new Row("Alice", 1),
                        new Row("Bob", 1)
                )
        );
    }

    @Test
    public void test_groupExpressionHavingExpression() {
        String name = createTable(
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "4"},
                new String[]{"Alice", "2"},
                new String[]{"Joey", "2"}
        );

        assertRowsAnyOrder(
                "SELECT name, (distance * 2) d FROM " + name + " GROUP BY name, d HAVING d > 2",
                asList(
                        new Row("Alice", 4L),
                        new Row("Alice", 8L),
                        new Row("Joey", 4L)
                )
        );
    }

    @Test
    public void test_count() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"}
        );

        assertRowsAnyOrder(
                "SELECT COUNT(*) FROM " + name,
                singletonList(new Row(2L))
        );
    }

    @Test
    public void test_emptyCount() {
        String name = createTable();

        assertRowsAnyOrder(
                "SELECT COUNT(*) FROM " + name,
                singletonList(new Row(0L))
        );
    }

    @Test
    public void test_groupCount() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, COUNT(*) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", 3L),
                        new Row("Bob", 1L)
                )
        );
    }

    @Test
    public void test_groupExpressionCount() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Alice", "1"},
                new String[]{"Joey", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, (distance * 2) d, COUNT(*) FROM " + name + " GROUP BY name, d",
                asList(
                        new Row("Alice", 2L, 2L),
                        new Row("Alice", 4L, 1L),
                        new Row("Bob", 2L, 1L),
                        new Row("Joey", 2L, 1L)
                )
        );
    }

    @Test
    public void test_groupCountHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, COUNT(*) c FROM " + name + " GROUP BY name, distance HAVING c < 2",
                asList(
                        new Row("Alice", 1L),
                        new Row("Bob", 1L)
                )
        );
    }

    @Test
    public void test_groupExpressionCountHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, (distance * 2) d, COUNT(*) c FROM " + name + " GROUP BY name, d HAVING c < 2",
                asList(
                        new Row("Alice", 4L, 1L),
                        new Row("Bob", 2L, 1L)
                )
        );
    }

    @Test
    public void test_min() {
        String name = createTable(
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Joey", "null"}
        );

        assertRowsAnyOrder(
                "SELECT MIN(name), MIN(distance) FROM " + name,
                singletonList(new Row("Alice", 1))
        );
    }

    @Test
    public void test_emptyMin() {
        String name = createTable();

        assertRowsAnyOrder(
                "SELECT MIN(name), MIN(distance) FROM " + name,
                singletonList(new Row(null, null))
        );
    }

    @Test
    public void test_groupMin() {
        String name = createTable(
                new String[]{"Alice", "2"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, MIN(distance) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", 1),
                        new Row("Bob", 2)
                )
        );
    }

    @Test
    public void test_groupExpressionMin() {
        String name = createTable(
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, MIN(distance * 2) m FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", 2L),
                        new Row("Bob", 2L)
                )
        );
    }

    @Test
    public void test_groupMinHaving() {
        String name = createTable(
                new String[]{"Joey", "3"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, MIN(distance) m FROM " + name + " GROUP BY name HAVING m > 1",
                asList(
                        new Row("Bob", 2),
                        new Row("Joey", 3)
                )
        );
    }

    @Test
    public void test_groupExpressionMinHaving() {
        String name = createTable(
                new String[]{"Joey", "3"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, MIN(distance * 2) m FROM " + name + " GROUP BY name HAVING m < 5",
                asList(
                        new Row("Alice", 2L),
                        new Row("Bob", 2L)
                )
        );
    }

    @Test
    public void test_max() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", "null"}
        );

        assertRowsAnyOrder(
                "SELECT MAX(name), MAX(distance) FROM " + name,
                singletonList(new Row("Joey", 2))
        );
    }

    @Test
    public void test_emptyMax() {
        String name = createTable();

        assertRowsAnyOrder(
                "SELECT MAX(name), MAX(distance) FROM " + name,
                singletonList(new Row(null, null))
        );
    }

    @Test
    public void test_groupMax() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "2"}
        );

        assertRowsAnyOrder(
                "SELECT name, MAX(distance) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", 2),
                        new Row("Bob", 2)
                )
        );
    }

    @Test
    public void test_groupExpressionMax() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"}
        );

        assertRowsAnyOrder(
                "SELECT name, MAX(distance * 2) m FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", 4L),
                        new Row("Bob", 2L)
                )
        );
    }

    @Test
    public void test_groupMaxHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT name, MAX(distance) m FROM " + name + " GROUP BY name HAVING m > 1",
                asList(
                        new Row("Alice", 2),
                        new Row("Joey", 3)
                )
        );
    }

    @Test
    public void test_groupExpressionMaxHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT name, MAX(distance * 2) m FROM " + name + " GROUP BY name HAVING m < 5",
                asList(
                        new Row("Alice", 4L),
                        new Row("Bob", 4L)
                )
        );
    }

    @Test
    public void test_sum() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", "null"}
        );

        assertRowsAnyOrder(
                "SELECT SUM(distance) FROM " + name,
                singletonList(new Row(3L))
        );
    }

    @Test
    public void test_emptySum() {
        String name = createTable();

        assertRowsAnyOrder(
                "SELECT SUM(distance) FROM " + name,
                singletonList(new Row((Object) null))
        );
    }

    @Test
    public void test_groupSum() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "2"}
        );

        assertRowsAnyOrder(
                "SELECT name, SUM(distance) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", 3L),
                        new Row("Bob", 2L)
                )
        );
    }

    @Test
    public void test_groupExpressionSum() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"}
        );

        assertRowsAnyOrder(
                "SELECT name, SUM(distance * 2) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", 6L),
                        new Row("Bob", 2L)
                )
        );
    }

    @Test
    public void test_groupSumHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "2"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT name, SUM(distance) s FROM " + name + " GROUP BY name HAVING s > 2",
                asList(
                        new Row("Alice", 3L),
                        new Row("Joey", 3L)
                )
        );
    }

    @Test
    public void test_groupExpressionSumHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT name, SUM(distance * 2) s FROM " + name + " GROUP BY name HAVING s > 4",
                asList(
                        new Row("Alice", 6L),
                        new Row("Joey", 6L)
                )
        );
    }

    @Test
    public void test_avg() {
        String name = createTable(
                new String[]{"Alice", "4"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", "null"}
        );

        assertRowsAnyOrder(
                "SELECT AVG(distance) FROM " + name,
                singletonList(new Row(new BigDecimal("3")))
        );
    }

    @Test
    public void test_emptyAvg() {
        String name = createTable();

        assertRowsAnyOrder(
                "SELECT AVG(distance) FROM " + name,
                singletonList(new Row((Object) null))
        );
    }

    @Test
    public void test_groupAvg() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "2"}
        );

        assertRowsAnyOrder(
                "SELECT name, AVG(distance) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", new BigDecimal("1.5")),
                        new Row("Bob", new BigDecimal("2"))
                )
        );
    }

    @Test
    public void test_groupExpressionAvg() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"}
        );

        assertRowsAnyOrder(
                "SELECT name, AVG(distance * 2) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", new BigDecimal("3")),
                        new Row("Bob", new BigDecimal("2"))
                )
        );
    }

    @Test
    public void test_groupAvgHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "3"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT name, AVG(distance) s FROM " + name + " GROUP BY name HAVING s > 2",
                asList(
                        new Row("Bob", new BigDecimal("2.5")),
                        new Row("Joey", new BigDecimal("3"))
                )
        );
    }

    @Test
    public void test_groupExpressionAvgHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "3"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT name, AVG(distance * 2) s FROM " + name + " GROUP BY name HAVING s > 4",
                asList(
                        new Row("Bob", new BigDecimal("5")),
                        new Row("Joey", new BigDecimal("6"))
                )
        );
    }

    @Test
    public void test_multipleAggregateFunctions() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT COUNT(*), MIN(distance), MAX(distance), SUM(distance), AVG(distance) FROM " + name,
                singletonList(new Row(3L, 1, 3, 6L, new BigDecimal("2")))
        );
    }

    private static String createTable(String[]... values) {
        String name = "aggregate_" + randomString().replace('-', '_');
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("name", "distance"),
                asList(QueryDataType.VARCHAR, QueryDataType.INT),
                asList(values)
        );
        return name;
    }
}
