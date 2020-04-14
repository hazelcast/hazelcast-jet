package com.hazelcast.jet.sql;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.sql.impl.schema.JetSchema;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector.TO_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector.TO_VALUE_CLASS;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class SqlInsertTest extends SimpleTestInClusterSupport {

    private static final String PERSON_MAP_SOURCE = "person_map_src";
    private static final String PERSON_MAP_SINK = "person_map_sink";

    private static final String PERSON_WITH_DATE_MAP_SINK = "person_with_date_map_sink";

    private static JetSqlService sqlService;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
        sqlService = new JetSqlService(instance());

        sqlService.createTable(PERSON_MAP_SOURCE, JetSchema.IMAP_LOCAL_SERVER,
                ImmutableMap.of(TO_VALUE_CLASS, PersonWithDate.class.getName()),
                asList(
                        entry("__key", QueryDataType.INT),
                        entry("birthday", QueryDataType.DATE),
                        entry("height", QueryDataType.INT)
                )
        );

        sqlService.createTable(PERSON_MAP_SINK, JetSchema.IMAP_LOCAL_SERVER,
                ImmutableMap.of(TO_KEY_CLASS, Person.class.getName(), TO_VALUE_CLASS, Person.class.getName()),
                asList(
                        entry("birthday", QueryDataType.DATE),
                        entry("height", QueryDataType.INT)
                )
        );

        sqlService.createTable(PERSON_WITH_DATE_MAP_SINK, JetSchema.IMAP_LOCAL_SERVER,
                ImmutableMap.of(TO_VALUE_CLASS, PersonWithDate.class.getName()),
                asList(
                        entry("__key", QueryDataType.INT),
                        entry("birthday", QueryDataType.DATE),
                        entry("height", QueryDataType.INT)
                )
        );
    }

    @Test
    public void map_value_shadows_key() {
        assertMap(
                PERSON_MAP_SINK, "INSERT INTO " + PERSON_MAP_SINK + "(birthday, height) VALUES ('2020-01-01', 150)",
                createMap(new Person(), new Person(LocalDate.of(2020, 1, 1), 150)));
    }

    @Test
    public void insert_select_converts_types() {
        Map<Integer, Person> map = instance().getMap(PERSON_MAP_SOURCE);
        map.put(1, new Person(LocalDate.of(2020, 1, 1), 150));

        assertMap(
                PERSON_WITH_DATE_MAP_SINK, "INSERT INTO " + PERSON_WITH_DATE_MAP_SINK + " SELECT * FROM " + PERSON_MAP_SOURCE,
                createMap(1, new PersonWithDate(new Date(120, Calendar.JANUARY, 1), 150)));
    }

    @Test
    public void insert_converts_types() {
        assertMap(
                PERSON_WITH_DATE_MAP_SINK, "INSERT INTO " + PERSON_WITH_DATE_MAP_SINK + "(__key, birthday, height) VALUES (1, '2020-01-01', 150)",
                createMap(1, new PersonWithDate(new Date(120, 0, 1), 150)));
    }

    private <K, V> void assertMap(String name, String sql, Map<K, V> expected) {
        sqlService.execute(sql).join();
        assertEquals(expected, new HashMap<>(instance().getMap(name)));
    }

    @SuppressWarnings("unused")
    public static class Person implements Serializable {

        private LocalDate birthday;

        private int height;

        public Person() {
        }

        private Person(LocalDate birthday, int height) {
            this.birthday = birthday;
            this.height = 150;
        }

        public LocalDate getBirthday() {
            return birthday;
        }

        public void setBirthday(LocalDate birthday) {
            this.birthday = birthday;
        }

        public int getHeight() {
            return height;
        }

        public void setHeight(int height) {
            this.height = height;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "birthday=" + birthday +
                    ", height=" + height +
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
            return height == person.height &&
                    Objects.equals(birthday, person.birthday);
        }

        @Override
        public int hashCode() {
            return Objects.hash(birthday, height);
        }
    }

    @SuppressWarnings("unused")
    public static class PersonWithDate implements Serializable {

        private Date birthday;
        private int height;

        public PersonWithDate() {
        }

        private PersonWithDate(Date birthday, int height) {
            this.birthday = birthday;
            this.height = height;
        }

        public Date getBirthday() {
            return birthday;
        }

        public void setBirthday(Date birthday) {
            this.birthday = birthday;
        }

        public int getHeight() {
            return height;
        }

        public void setHeight(int height) {
            this.height = height;
        }

        @Override
        public String toString() {
            return "PersonWithDate{" +
                    "birthday=" + birthday +
                    ", height=" + height +
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
            PersonWithDate that = (PersonWithDate) o;
            return height == that.height &&
                    Objects.equals(birthday, that.birthday);
        }

        @Override
        public int hashCode() {
            return Objects.hash(birthday, height);
        }
    }
}
