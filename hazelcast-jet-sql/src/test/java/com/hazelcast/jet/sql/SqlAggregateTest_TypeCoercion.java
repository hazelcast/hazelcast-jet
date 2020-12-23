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
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.jet.sql.impl.connector.test.AllTypesSqlConnector.ALL_TYPES_ROW;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlAggregateTest_TypeCoercion extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
        sqlService = instance().getSql();
    }

    @Before
    public void before() {
        AllTypesSqlConnector.create(sqlService, "allTypesTable");
    }

    @Test
    public void test_count() {
        assertRowsAnyOrder("select " +
                        "count(string), " +
                        "count(\"boolean\"), " +
                        "count(byte), " +
                        "count(short), " +
                        "count(\"int\"), " +
                        "count(long), " +
                        "count(\"float\"), " +
                        "count(\"double\"), " +
                        "count(\"decimal\")," +
                        "count(\"time\")," +
                        "count(\"date\")," +
                        "count(\"timestamp\")," +
                        "count(\"timestampTz\")," +
                        "count(\"object\")," +
                        "count(null) " +
                        "from allTypesTable",
                Collections.singleton(new Row(1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 0L, 0L)));
    }

    @Test
    public void test_sum_numberTypes() {
        assertRowsAnyOrder("select " +
                "sum(byte), " +
                "sum(short), " +
                "sum(\"int\"), " +
                "sum(long), " +
                "sum(\"float\"), " +
                "sum(\"double\"), " +
                "sum(\"decimal\")," +
                "sum(null) " +
                "from allTypesTable",
                Collections.singleton(new Row(
                        127L,
                        32767L,
                        2147483647L,
                        9223372036854775807L,
                        (double) 1234567890.1f,
                        123451234567890.1,
                        new BigDecimal("9223372036854775.123"),
                        null)));
    }

    @Test
    public void test_sum_otherTypes() {
        assertThatThrownBy(() -> sqlService.execute("select sum(string) from allTypesTable"))
                .hasMessageContaining("Cannot apply 'SUM' function to [VARCHAR]");
        assertThatThrownBy(() -> sqlService.execute("select sum(\"boolean\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'SUM' function to [BOOLEAN]");
        assertThatThrownBy(() -> sqlService.execute("select sum(\"time\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'SUM' function to [TIME]");
        assertThatThrownBy(() -> sqlService.execute("select sum(\"date\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'SUM' function to [DATE]");
        assertThatThrownBy(() -> sqlService.execute("select sum(\"timestamp\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'SUM' function to [TIMESTAMP]");
        assertThatThrownBy(() -> sqlService.execute("select sum(\"timestampTz\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'SUM' function to [TIMESTAMP_WITH_TIME_ZONE]");
        assertThatThrownBy(() -> sqlService.execute("select sum(\"object\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'SUM' function to [OBJECT]");
    }

    @Test
    public void test_sum_integerOverflow() {
        TestBatchSqlConnector.create(sqlService, "t", singletonList("a"), singletonList(QueryDataType.BIGINT),
                asList(
                        new String[]{"" + Long.MAX_VALUE},
                        new String[]{"" + Long.MAX_VALUE}));

        assertThatThrownBy(() -> sqlService.execute("select sum(a) from t").iterator().hasNext())
                .hasMessageContaining("BIGINT overflow in 'SUM' function");

        // casted to DECIMAL should work
        assertRowsAnyOrder("select sum(cast(a as decimal)) from t",
                singletonList(new Row(new BigDecimal(Long.MAX_VALUE).multiply(new BigDecimal(2)))));
    }

    @Test
    public void test_avg_numericTypes() {
        assertRowsAnyOrder("select " +
                        "avg(byte), " +
                        "avg(short), " +
                        "avg(\"int\"), " +
                        "avg(long), " +
                        "avg(\"float\"), " +
                        "avg(\"double\"), " +
                        "avg(\"decimal\")," +
                        "avg(null) " +
                        "from allTypesTable",
                Collections.singleton(new Row(
                        BigDecimal.valueOf(127),
                        BigDecimal.valueOf(32767),
                        BigDecimal.valueOf(2147483647),
                        BigDecimal.valueOf(9223372036854775807L),
                        (double) 1234567890.1f,
                        123451234567890.1,
                        new BigDecimal("9223372036854775.123"),
                        null)));
    }

    @Test
    public void test_avg_otherTypes() {
        assertThatThrownBy(() -> sqlService.execute("select avg(string) from allTypesTable"))
                .hasMessageContaining("Cannot apply 'AVG' function to [VARCHAR]");
        assertThatThrownBy(() -> sqlService.execute("select avg(\"boolean\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'AVG' function to [BOOLEAN]");
        assertThatThrownBy(() -> sqlService.execute("select avg(\"time\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'AVG' function to [TIME]");
        assertThatThrownBy(() -> sqlService.execute("select avg(\"date\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'AVG' function to [DATE]");
        assertThatThrownBy(() -> sqlService.execute("select avg(\"timestamp\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'AVG' function to [TIMESTAMP]");
        assertThatThrownBy(() -> sqlService.execute("select avg(\"timestampTz\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'AVG' function to [TIMESTAMP_WITH_TIME_ZONE]");
        assertThatThrownBy(() -> sqlService.execute("select avg(\"object\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'AVG' function to [OBJECT]");
    }

    @Test
    public void test_avg_integerOverflow() {
        TestBatchSqlConnector.create(sqlService, "t", singletonList("a"), singletonList(QueryDataType.BIGINT),
                asList(
                        new String[]{"" + Long.MAX_VALUE},
                        new String[]{"" + Long.MAX_VALUE}));

        List<Row> expectedResult = singletonList(new Row(new BigDecimal(Long.MAX_VALUE)));

        // we don't have AVG(bigint), only AVG(decimal), therefore it won't overflow
        assertRowsAnyOrder("select avg(a) from t", expectedResult);

        // explicitly casted to DECIMAL won't overflow too
        assertRowsAnyOrder("select avg(cast(a as decimal)) from t", expectedResult);
    }

    @Test
    public void test_min() {
        assertRowsAnyOrder("select " +
                        "min(string), " +
                        "min(\"boolean\"), " +
                        "min(byte), " +
                        "min(short), " +
                        "min(\"int\"), " +
                        "min(long), " +
                        "min(\"float\"), " +
                        "min(\"double\"), " +
                        "min(\"decimal\")," +
                        "min(\"time\")," +
                        "min(\"date\")," +
                        "min(\"timestamp\")," +
                        "min(\"timestampTz\")," +
                        "min(\"object\") " +
                        "from allTypesTable",
                Collections.singleton(ALL_TYPES_ROW));

        assertRowsAnyOrder("select min(null) from allTypesTable", singletonList(new Row((Object) null)));
    }

    @Test
    public void test_max() {
        assertRowsAnyOrder("select " +
                        "max(string), " +
                        "max(\"boolean\"), " +
                        "max(byte), " +
                        "max(short), " +
                        "max(\"int\"), " +
                        "max(long), " +
                        "max(\"float\"), " +
                        "max(\"double\"), " +
                        "max(\"decimal\")," +
                        "max(\"time\")," +
                        "max(\"date\")," +
                        "max(\"timestamp\")," +
                        "max(\"timestampTz\")," +
                        "max(\"object\") " +
                        "from allTypesTable",
                Collections.singleton(ALL_TYPES_ROW));

        assertRowsAnyOrder("select max(null) from allTypesTable", singletonList(new Row((Object) null)));
    }
}
