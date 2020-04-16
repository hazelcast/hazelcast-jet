package com.hazelcast.jet.sql;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.sql.impl.schema.JetSchema;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector.TO_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector.TO_VALUE_CLASS;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class SqlInsertTest extends SimpleTestInClusterSupport {

    private static final String PERSON_MAP_SINK = "person_map_sink";


    private static JetSqlService sqlService;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
        sqlService = new JetSqlService(instance());

        sqlService.createTable(PERSON_MAP_SINK, JetSchema.IMAP_LOCAL_SERVER,
                ImmutableMap.of(TO_KEY_CLASS, Person.class.getName(), TO_VALUE_CLASS, Person.class.getName()),
                singletonList(
                        entry("birthday", QueryDataType.DATE)
                )
        );
    }

    @Test
    public void map_value_shadows_key() {
        assertMap(
                PERSON_MAP_SINK, "INSERT INTO " + PERSON_MAP_SINK + "(birthday) VALUES ('2020-01-01')",
                createMap(new Person(), new Person(LocalDate.of(2020, 1, 1))));
    }

    private <K, V> void assertMap(String name, String sql, Map<K, V> expected) {
        sqlService.execute(sql).join();
        assertEquals(expected, new HashMap<>(instance().getMap(name)));
    }

    @SuppressWarnings("unused")
    public static class Person implements Serializable {

        private LocalDate birthday;

        public Person() {
        }

        private Person(LocalDate birthday) {
            this.birthday = birthday;
        }

        public LocalDate getBirthday() {
            return birthday;
        }

        public void setBirthday(LocalDate birthday) {
            this.birthday = birthday;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "birthday=" + birthday +
                    '}';
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
            return Objects.equals(birthday, person.birthday);
        }

        @Override
        public int hashCode() {
            return Objects.hash(birthday);
        }
    }
}
