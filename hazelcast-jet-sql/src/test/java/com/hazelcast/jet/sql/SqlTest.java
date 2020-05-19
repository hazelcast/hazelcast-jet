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
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.connector.LocalPartitionedMapConnector;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector.TO_VALUE_CLASS;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public class SqlTest extends SimpleTestInClusterSupport {

    private static final String INT_TO_STRING_MAP_SRC = "int_to_string_map_src";
    private static final String INT_TO_STRING_MAP_SINK = "int_to_string_map_sink";

    private static final String PERSON_MAP_SRC = "person_map_src";
    private static final String PERSON_MAP_SINK = "person_map_sink";

    private static final String BIG_INTEGER_TO_CHAR_MAP = "big_integer_to_char_map";

    private static final Person PERSON_ALICE = new Person("Alice", 30);
    private static final Person PERSON_BOB = new Person("Bob", 40);
    private static final Person PERSON_CECILE = new Person("Cecile", 50);

    private static SqlService sqlService;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);

        sqlService = instance().getHazelcastInstance().getSqlService();

        sqlService.query(format("CREATE EXTERNAL TABLE %s (__key INT, this VARCHAR) TYPE \"%s\"",
                INT_TO_STRING_MAP_SRC, LocalPartitionedMapConnector.TYPE_NAME));
        sqlService.query(format("CREATE EXTERNAL TABLE %s (__key INT, this VARCHAR) TYPE \"%s\"",
                INT_TO_STRING_MAP_SINK, LocalPartitionedMapConnector.TYPE_NAME));

        sqlService.query(format("CREATE EXTERNAL TABLE %s (__key INT, name VARCHAR, age INT) TYPE \"%s\" OPTIONS (%s '%s')",
                PERSON_MAP_SRC, LocalPartitionedMapConnector.TYPE_NAME, TO_VALUE_CLASS, Person.class.getName()));
        sqlService.query(format("CREATE EXTERNAL TABLE %s (__key INT, name VARCHAR, age INT) TYPE \"%s\" OPTIONS (%s '%s')",
                PERSON_MAP_SINK, LocalPartitionedMapConnector.TYPE_NAME, TO_VALUE_CLASS, Person.class.getName()));

        sqlService.query(format("CREATE EXTERNAL TABLE %s (__key DECIMAL(10, 0), this CHAR) TYPE \"%s\"",
                BIG_INTEGER_TO_CHAR_MAP, LocalPartitionedMapConnector.TYPE_NAME));
    }

    @Before
    public void before() {
        IMap<Integer, String> intToStringMap = instance().getMap(INT_TO_STRING_MAP_SRC);
        intToStringMap.put(0, "value-0");
        intToStringMap.put(1, "value-1");
        intToStringMap.put(2, "value-2");

        IMap<Integer, Person> personMap = instance().getMap(PERSON_MAP_SRC);
        personMap.put(0, PERSON_ALICE);
        personMap.put(1, PERSON_BOB);
        personMap.put(2, PERSON_CECILE);
    }

    @Test
    public void fullScan() {
        assertRowsAnyOrder(
                "SELECT this, __key FROM " + INT_TO_STRING_MAP_SRC,
                asList(
                        new Row("value-0", 0),
                        new Row("value-1", 1),
                        new Row("value-2", 2)));
    }

    @Test
    public void fullScan_person() {
        assertRowsAnyOrder(
                "SELECT __key, name, age FROM " + PERSON_MAP_SRC + " p",
                asList(
                        new Row(0, PERSON_ALICE.getName(), PERSON_ALICE.getAge()),
                        new Row(1, PERSON_BOB.getName(), PERSON_BOB.getAge()),
                        new Row(2, PERSON_CECILE.getName(), PERSON_CECILE.getAge())));
    }

    @Test
    public void fullScan_star() {
        assertRowsAnyOrder(
                "SELECT * FROM " + INT_TO_STRING_MAP_SRC,
                asList(
                        new Row(0, "value-0"),
                        new Row(1, "value-1"),
                        new Row(2, "value-2")));
    }

    @Test
    public void fullScan_filter() {
        assertRowsAnyOrder(
                "SELECT this FROM " + INT_TO_STRING_MAP_SRC + " WHERE __key=1 or this='value-0'",
                asList(new Row("value-1"), new Row("value-0")));
    }

    @Test
    public void fullScan_projection() {
        assertRowsAnyOrder(
                "SELECT upper(this) FROM " + INT_TO_STRING_MAP_SRC + " WHERE this='value-1'",
                singletonList(new Row("VALUE-1")));
    }

    @Test
    public void fullScan_projection2() {
        assertRowsAnyOrder(
                "SELECT this FROM " + INT_TO_STRING_MAP_SRC + " WHERE upper(this)='VALUE-1'",
                singletonList(new Row("value-1")));
    }

    @Test
    public void fullScan_projection3() {
        assertRowsAnyOrder(
                "SELECT this FROM (SELECT upper(this) this FROM " + INT_TO_STRING_MAP_SRC + ") WHERE this='VALUE-1'",
                singletonList(new Row("VALUE-1")));
    }

    @Test
    public void fullScan_projection4() {
        assertRowsAnyOrder(
                "SELECT upper(this) FROM " + INT_TO_STRING_MAP_SRC + " WHERE upper(this)='VALUE-1'",
                singletonList(new Row("VALUE-1")));
    }

    @Test
    public void selectWithoutFrom_unicode() {
        assertRowsAnyOrder(
                "SELECT '喷气式飞机'",
                singletonList(new Row("喷气式飞机")));
    }

    @Test
    public void selectWithConversion() {
        SqlCursor cursor = sqlService.query("INSERT OVERWRITE " + BIG_INTEGER_TO_CHAR_MAP + " VALUES (12, 'a')");
        cursor.iterator().forEachRemaining(o -> { });

        assertRowsAnyOrder(
                "SELECT __key + 1, this FROM " + BIG_INTEGER_TO_CHAR_MAP,
                singletonList(new Row(BigDecimal.valueOf(13), 'a')));
    }

    @Test
    public void insert() {
        assertMap(
                INT_TO_STRING_MAP_SINK, "INSERT OVERWRITE " + INT_TO_STRING_MAP_SINK + " SELECT * FROM " + INT_TO_STRING_MAP_SRC,
                createMap(
                        0, "value-0",
                        1, "value-1",
                        2, "value-2"));
    }

    @Test
    public void insert_values() {
        assertMap(
                INT_TO_STRING_MAP_SINK, "INSERT OVERWRITE " + INT_TO_STRING_MAP_SINK + "(this, __key) values (2, 1)",
                createMap(1, "2"));
    }

    @Test
    public void insert_person() {
        assertMap(
                PERSON_MAP_SINK, "INSERT OVERWRITE " + PERSON_MAP_SINK + " VALUES (1, 'Foo', 25)",
                createMap(
                        1, new Person("Foo", 25)));
    }

    private <K, V> void assertMap(String mapName, String sql, Map<K, V> expected) {
        SqlCursor cursor = sqlService.query(sql);
        cursor.iterator().forEachRemaining(o -> { });
        assertEquals(expected, new HashMap<>(instance().getMap(mapName)));
    }

    private void assertRowsAnyOrder(String sql, Collection<Row> expectedRows) {
        SqlCursor cursor = sqlService.query(sql);

        Set<Row> result = StreamSupport.stream(cursor.spliterator(), false).map(Row::new).collect(toSet());
        assertEquals(new HashSet<>(expectedRows), result);
    }

    private static final class Row {

        Object[] values;

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
            Row row = (Row) o;
            return Arrays.equals(values, row.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }

    @SuppressWarnings("unused")
    public static final class Person implements Serializable {

        private String name;
        private int age;

        public Person() {
        }

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        @SuppressWarnings("unused") // used through reflection
        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        @SuppressWarnings("unused") // used through reflection
        public void setAge(int age) {
            this.age = age;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Person person = (Person) o;
            return age == person.age &&
                    Objects.equals(name, person.name);
        }

        @Override
        public String toString() {
            return "Person{name='" + name + '\'' + ", age=" + age + '}';
        }
    }
}
