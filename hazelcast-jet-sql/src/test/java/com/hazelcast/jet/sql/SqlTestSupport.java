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
import com.hazelcast.jet.sql.impl.connector.map.LocalPartitionedMapConnector;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;

import javax.annotation.Nonnull;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.sql.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_VALUE_CLASS;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class SqlTestSupport extends SimpleTestInClusterSupport {

    protected static final String RESOURCES_PATH = Paths.get("src/test/resources").toFile().getAbsolutePath();

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getHazelcastInstance().getSql();
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
        List<Row> actualRows = executeSqlWithLimit(sql, expectedRows.size(), 10);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    protected static SqlResult executeSql(String sql) {
        return sqlService.query(sql);
    }

    /**
     * Execute a query and fetch up to {@code rowCountLimit} rows, then close
     * the result. Don't wait until the result iterator is exhausted if enough
     * rows were fetched. Suitable for streaming queries that don't stop. The
     * outstanding rows are ignored.
     */
    @Nonnull
    private static List<Row> executeSqlWithLimit(String sql, int rowCountLimit, int timeoutSeconds) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Deque<Row> rows = new ArrayDeque<>();

        Thread thread = new Thread(() -> {
            try (SqlResult result = sqlService.query(sql)) {
                Iterator<SqlRow> iterator = result.iterator();
                for (int i = 0; i < rowCountLimit && iterator.hasNext(); i++) {
                    rows.add(new Row(result.getRowMetadata().getColumnCount(), iterator.next()));
                }
                future.complete(null);
            } catch (RuntimeException e) {
                e.printStackTrace();
                future.completeExceptionally(e);
            }
        });

        thread.start();

        try {
            try {
                future.get(timeoutSeconds, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                thread.interrupt();
                thread.join();
            }
        } catch (Exception e) {
            throw sneakyThrow(e);
        }

        // return whichever rows we've collected so far
        return new ArrayList<>(rows);
    }

    /**
     * Create DDL for an IMap with the given {@code name}, that uses
     * java serialization for both key and value with the given classes.
     */
    public static String javaSerializableMapDdl(String name, Class<?> keyClass, Class<?> valueClass) {
        return "CREATE EXTERNAL TABLE " + name + " TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\"\n"
                + "OPTIONS (\n"
                + '"' + OPTION_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "',\n"
                + '"' + OPTION_KEY_CLASS + "\" '" + keyClass.getName() + "',\n"
                + '"' + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "',\n"
                + '"' + OPTION_VALUE_CLASS + "\" '" + valueClass.getName() + "'\n"
                + ")";
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
