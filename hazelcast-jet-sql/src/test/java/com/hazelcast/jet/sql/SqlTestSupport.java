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
import com.hazelcast.sql.SqlResult;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class SqlTestSupport extends SimpleTestInClusterSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getHazelcastInstance().getSqlService();
    }

    protected static <K, V> void assertMapEventually(String name, String sql, Map<K, V> expected) {
        executeSql(sql);

        assertTrueEventually(
                null,
                () -> assertThat(new HashMap<>(instance().getMap(name))).containsExactlyEntriesOf(expected),
                5
        );
    }

    protected static void assertRowsEventuallyAnyOrder(String sql, Collection<Row> expectedRows) {
        try {
            List<Row> actualRows = spawn(() -> executeSql(sql, expectedRows.size()))
                    .get(5, TimeUnit.SECONDS);

            assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectedRows);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw sneakyThrow(e);
        }
    }

    protected static void executeSql(String sql) {
        try (SqlResult cursor = toCursor(sql)) {
            cursor.iterator().forEachRemaining(o -> { });
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    private static List<Row> executeSql(String sql, int numberOfExpectedRows) {
        try (SqlResult cursor = toCursor(sql)) {
            Iterator<SqlRow> iterator = cursor.iterator();
            List<Row> rows = new ArrayList<>(numberOfExpectedRows);
            for (int i = 0; i < numberOfExpectedRows; i++) {
                rows.add(new Row(cursor.getRowMetadata().getColumnCount(), iterator.next()));
            }
            return rows;
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    private static SqlResult toCursor(String sql) {
        return sqlService.query(sql);
    }

    protected static final class Row {

        private final Object[] values;

        private Row(int columnCount, SqlRow row) {
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
            Row row = (Row) o;
            return Arrays.equals(values, row.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }
}
