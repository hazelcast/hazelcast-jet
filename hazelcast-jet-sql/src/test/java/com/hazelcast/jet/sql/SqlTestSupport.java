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

import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class SqlTestSupport extends SimpleTestInClusterSupport {

    protected static void assertRowsInAnyOrder(String sql, Row... expectedRows) {
        SqlService sqlService = instance().getSql();
        try (SqlResult result = sqlService.execute(sql)) {
            List<Row> actualRows = stream(result.spliterator(), false)
                    .map(row -> new Row(result.getRowMetadata().getColumnCount(), row))
                    .collect(toList());
            assertThat(actualRows).containsExactlyInAnyOrderElementsOf(asList(expectedRows));
        }
    }

    /**
     * A class passed to utility methods in this class. We don't use SqlRow
     * directly because:
     * - SqlRow doesn't implement `equals`
     * - It's not easy to create SqlRow instance
     */
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
