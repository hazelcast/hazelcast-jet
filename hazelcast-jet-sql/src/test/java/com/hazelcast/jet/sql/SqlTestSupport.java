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

import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.sql.impl.connector.kafka.SqlKafkaTest;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class SqlTestSupport extends SimpleTestInClusterSupport {

    private static SqlService sqlService;
    private static Executor executor;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getHazelcastInstance().getSqlService();
        executor = Executors.newSingleThreadExecutor();
    }

    protected static <K, V> void assertMap(String name, String sql, Map<K, V> expected) {
        executeSql(sql);

        assertThat(new HashMap<>(instance().getMap(name))).containsExactlyInAnyOrderEntriesOf(expected);
    }

    protected static void assertRowsEventuallyAnyOrder(String sql, Collection<Row> expectedRows) {
        try {
            List<Row> actualRows = supplyAsync(() -> executeSql(sql, expectedRows.size()), executor)
                    .get(5, TimeUnit.SECONDS);

            assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectedRows);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw sneakyThrow(e);
        }
    }

    protected static void executeSql(String sql) {
        try (SqlCursor cursor = toCursor(sql)) {
            cursor.iterator().forEachRemaining(o -> { });
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    private static List<Row> executeSql(String sql, int numberOfExpectedRows) {
        try (SqlCursor cursor = toCursor(sql)) {
            Iterator<SqlRow> iterator = cursor.iterator();
            List<Row> rows = new ArrayList<>(numberOfExpectedRows);
            for (int i = 0; i < numberOfExpectedRows; i++) {
                rows.add(new Row(cursor.getColumnCount(), iterator.next()));
            }
            return rows;
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    private static SqlCursor toCursor(String sql) {
        return sqlService.query(sql);
    }

    protected static final class Row {

        private final Object[] values;

        public Row(int columnCount, SqlRow row) {
            values = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                values[i] = row.getObject(i);
            }
        }

        public Row(Object... values) {
            this.values = values;
        }

        @Override
        public String toString() {
            return "Row{" + Arrays.toString(values) + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SqlKafkaTest.Row row = (SqlKafkaTest.Row) o;
            return Arrays.equals(values, row.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }
}
