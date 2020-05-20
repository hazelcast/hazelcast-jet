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
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.SqlRowImpl;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public abstract class SqlTestSupport extends SimpleTestInClusterSupport {

    protected static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getHazelcastInstance().getSqlService();
    }

    protected static <K, V> void assertMap(String name, String sql, Map<K, V> expected) {
        SqlCursor cursor = sqlService.query(sql);

        cursor.iterator().forEachRemaining(o -> { });

        assertEquals(expected, new HashMap<>(instance().getMap(name)));
    }

    protected static void assertRowsAnyOrder(String sql, Collection<Row> expectedRows) {
        SqlCursor cursor = sqlService.query(sql);

        Set<Row> actualRows = StreamSupport.stream(cursor.spliterator(), false).map(Row::new).collect(toSet());

        assertEquals(new HashSet<>(expectedRows), actualRows);
    }

    protected static void assertRowsEventuallyAnyOrder(String sql, Collection<Row> expectedRows) {
        SqlCursor cursor = sqlService.query(sql);

        Iterator<SqlRow> iterator = cursor.iterator();
        Set<Row> actualRows = new HashSet<>(expectedRows.size());
        for (int i = 0; i < expectedRows.size(); i++) {
            actualRows.add(new Row(cursor.getColumnCount(), iterator.next()));
        }

        assertEquals(new HashSet<>(expectedRows), actualRows);
    }

    protected static final class Row {

        Object[] values;

        Row(int columnCount, SqlRow row) {
            values = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                values[i] = row.getObject(i);
            }
        }

        Row(SqlRow sqlRow) {
            values = new Object[((SqlRowImpl) sqlRow).getDelegate().getColumnCount()];
            for (int i = 0; i < values.length; i++) {
                values[i] = sqlRow.getObject(i);
            }
        }

        Row(Object... values) {
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
