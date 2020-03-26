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

import com.hazelcast.jet.Job;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.sql.imap.IMapSqlConnector;
import com.hazelcast.jet.sql.schema.JetSchema;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.TestUtil.createMap;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public class SqlTest extends SimpleTestInClusterSupport {

    private static final String SINK_MAP = "sink_map";
    private static final String INT_TO_STRING_MAP = "int_to_string_map";

    private static JetSqlService sqlService;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
        sqlService = new JetSqlService(instance());

        sqlService.getSchema().createTable(INT_TO_STRING_MAP, JetSchema.IMAP_LOCAL_SERVER,
                emptyMap(),
                asList(entry("__key", QueryDataType.INT), entry("this", QueryDataType.VARCHAR)));

        sqlService.getSchema().createTable(SINK_MAP, JetSchema.IMAP_LOCAL_SERVER,
                createMap(IMapSqlConnector.TO_KEY_CLASS, Integer.class.getName(),
                        IMapSqlConnector.TO_VALUE_CLASS, String.class.getName()),
                asList(entry("__key", QueryDataType.INT), entry("this", QueryDataType.VARCHAR)));
    }

    @Before
    public void before() {
        IMap<Integer, String> intToStringMap = instance().getMap(INT_TO_STRING_MAP);
        intToStringMap.put(0, "value-0");
        intToStringMap.put(1, "value-1");
        intToStringMap.put(2, "value-2");
    }

    @Test
    public void fullScan() throws Exception {
        assertRowsAnyOrder(
                "SELECT this, __key FROM " + INT_TO_STRING_MAP,
                asList(
                        new Row("value-0", 0),
                        new Row("value-1", 1),
                        new Row("value-2", 2)));
    }

    @Test
    public void fullScan_star() throws Exception {
        assertRowsAnyOrder(
                "SELECT * FROM " + INT_TO_STRING_MAP,
                asList(
                        new Row(0, "value-0"),
                        new Row(1, "value-1"),
                        new Row(2, "value-2")));
    }

    @Test
    public void fullScan_filter() throws Exception {
        assertRowsAnyOrder(
                "SELECT this FROM " + INT_TO_STRING_MAP + " WHERE __key=1 or this='value-0'",
                asList(new Row("value-1"), new Row("value-0")));
    }

    @Test
    public void fullScan_projection() throws Exception {
        assertRowsAnyOrder(
                "SELECT upper(this) FROM " + INT_TO_STRING_MAP + " WHERE this='value-1'",
                singletonList(new Row("VALUE-1")));
    }

    @Test
    public void insert() {
        assertMap(
                "INSERT INTO " + SINK_MAP + " SELECT * FROM " + INT_TO_STRING_MAP,
                createMap(
                        0, "value-0",
                        1, "value-1",
                        2, "value-2"));
    }

    @Test
    public void insert_values() {
        assertMap(
                "INSERT INTO " + SINK_MAP + "(__key, this) values (1, 1)",
                createMap(1, "1"));
    }

    private <K, V> void assertMap(String sql, Map<K, V> expected) {
        Job job = sqlService.execute(sql);
        job.join();
        assertEquals(expected, new HashMap<>(instance().getMap(SINK_MAP)));
    }

    private void assertRowsAnyOrder(String sql, Collection<Row> expectedRows) throws Exception {
        Observable<Object[]> observable = sqlService.executeQuery(sql);

        List<Object[]> result = observable.toFuture(str -> str.collect(toList())).get();
        assertEquals(new HashSet<>(expectedRows), result.stream().map(Row::new).collect(toSet()));
    }

    private static final class Row {
        Object[] values;

        Row(Object ... values) {
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
