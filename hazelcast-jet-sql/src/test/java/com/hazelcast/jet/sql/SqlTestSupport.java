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
import com.hazelcast.jet.sql.impl.connector.SqlKafkaTest;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.SqlRowImpl;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class SqlTestSupport extends SimpleTestInClusterSupport {

    protected static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getHazelcastInstance().getSqlService();
    }

    protected static <K, V> void assertMap(String name, String sql, Map<K, V> expected) {
        try (SqlCursor cursor = toCursor(sql)) {
            cursor.iterator().forEachRemaining(o -> { });

            assertThat(new HashMap<>(instance().getMap(name))).containsExactlyInAnyOrderEntriesOf(expected);
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    protected static void assertRowsAnyOrder(String sql, Collection<Row> expectedRows) {
        try (SqlCursor cursor = toCursor(sql)) {
            List<Row> actualRows = stream(cursor.spliterator(), false).map(Row::new).collect(toList());

            assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectedRows);
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    protected static void assertRowsEventuallyAnyOrder(String sql, Collection<Row> expectedRows) {
        try (SqlCursor cursor = toCursor(sql)) {
            Iterator<SqlRow> iterator = cursor.iterator();
            List<Row> actualRows = new ArrayList<>(expectedRows.size());
            for (int i = 0; i < expectedRows.size(); i++) {
                actualRows.add(new Row(cursor.getColumnCount(), iterator.next()));
            }

            assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectedRows);
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

        public Row(SqlRow sqlRow) {
            values = new Object[((SqlRowImpl) sqlRow).getDelegate().getColumnCount()];
            for (int i = 0; i < values.length; i++) {
                values[i] = sqlRow.getObject(i);
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
