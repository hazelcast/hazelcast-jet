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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.Util;
import com.hazelcast.jet.sql.JetSqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.model.AllTypesValue;
import com.hazelcast.jet.sql.impl.connector.map.model.InsuredPerson;
import com.hazelcast.jet.sql.impl.connector.map.model.Person;
import com.hazelcast.jet.sql.impl.connector.map.model.PersonId;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_OBJECT_NAME;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_VALUE_CLASS;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// TODO: move it to IMDG when INSERTs are supported, or at least move to one of Jet connector tests ?
public class SqlPojoTest extends JetSqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void supportsPrimitiveInsertsIntoDiscoveredMap() {
        String name = generateRandomName();

        instance().getMap(name).put(BigInteger.valueOf(1), "Alice");

        assertMapEventually(
                name,
                "INSERT OVERWRITE partitioned." + name + " VALUES (2, 'Bob')",
                createMap(BigInteger.valueOf(1), "Alice", BigInteger.valueOf(2), "Bob")
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row(BigDecimal.valueOf(1), "Alice"),
                        new Row(BigDecimal.valueOf(2), "Bob")
                )
        );
    }

    @Test
    public void supportsPojoInsertsIntoDiscoveredMap() {
        String name = generateRandomName();

        instance().getMap(name).put(new PersonId(1), new Person(1, "Alice"));

        assertMapEventually(
                name,
                // TODO: requires explicit column list due to hidden fields...
                "INSERT OVERWRITE partitioned." + name + " (id, name) VALUES (2, 'Bob')",
                createMap(new PersonId(1), new Person(1, "Alice"), new PersonId(2), new Person(0, "Bob"))
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row(1, "Alice"),
                        new Row(2, "Bob")
                )
        );
    }

    @Test
    public void supportsNulls() {
        String name = createMapWithRandomName();

        assertMapEventually(
                name,
                "INSERT OVERWRITE " + name + " VALUES (null, null)",
                createMap(new PersonId(), new Person()));

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(0, null))
        );
    }

    @Test
    public void supportsFieldsShadowing() {
        String name = createMapWithRandomName();

        assertMapEventually(
                name,
                "INSERT OVERWRITE " + name + " (id, name) VALUES (1, 'Alice')",
                createMap(new PersonId(1), new Person(0, "Alice"))
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "Alice"))
        );
    }

    @Test
    public void supportsFieldsMapping() {
        String name = generateRandomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "key_id INT EXTERNAL NAME \"__key.id\""
                + ", value_id INT EXTERNAL NAME \"this.id\""
                + ") TYPE \"" + IMapSqlConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + OPTION_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_KEY_CLASS + "\" '" + PersonId.class.getName() + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_VALUE_CLASS + "\" '" + Person.class.getName() + "'"
                + ")"
        );

        assertMapEventually(
                name,
                "INSERT OVERWRITE " + name + " (value_id, key_id, name) VALUES (2, 1, 'Alice')",
                createMap(new PersonId(1), new Person(2, "Alice"))
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT key_id, value_id, name FROM " + name,
                singletonList(new Row(1, 2, "Alice"))
        );
    }

    @Test
    public void supportsSchemaEvolution() {
        String name = createMapWithRandomName();

        // insert initial record
        sqlService.execute("INSERT OVERWRITE " + name + " VALUES (1, 'Alice')");

        // alter schema
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + " "
                + "TYPE \"" + IMapSqlConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + OPTION_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_KEY_CLASS + "\" '" + PersonId.class.getName() + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_VALUE_CLASS + "\" '" + InsuredPerson.class.getName() + "'"
                + ")"
        );

        // insert record against new schema
        sqlService.execute("INSERT OVERWRITE " + name + " (id, name, ssn) VALUES (2, 'Bob', 123456789)");

        // assert both - initial & evolved - records are correctly read
        assertRowsEventuallyInAnyOrder(
                "SELECT id, name, ssn FROM " + name,
                asList(
                        new Row(1, "Alice", null),
                        new Row(2, "Bob", 123456789L)
                )
        );
    }

    @Test
    public void supportsFieldsExtensions() {
        String name = generateRandomName();

        Map<PersonId, InsuredPerson> map = instance().getMap(name);
        map.put(new PersonId(1), new InsuredPerson(1, "Alice", 123456789L));

        sqlService.execute("CREATE MAPPING " + name + " ("
                + "ssn BIGINT"
                + ") TYPE \"" + IMapSqlConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + OPTION_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_KEY_CLASS + "\" '" + PersonId.class.getName() + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_VALUE_CLASS + "\" '" + Person.class.getName() + "'"
                + ")"
        );

        assertMapEventually(
                name,
                "INSERT OVERWRITE " + name + " (id, name, ssn) VALUES (2, 'Bob', null)",
                createMap(
                        new PersonId(1), new InsuredPerson(1, "Alice", 123456789L),
                        new PersonId(2), new Person(0, "Bob")
                )
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row(1, "Alice", 123456789L),
                        new Row(2, "Bob", null)
                )
        );
    }

    @Test
    public void supportsAllTypes() {
        String name = generateRandomName();
        sqlService.execute(javaSerializableMapDdl(name, BigInteger.class, AllTypesValue.class));

        assertMapEventually(
                name,
                "INSERT OVERWRITE " + name + " ("
                        + "__key"
                        + ", string"
                        + ", character0"
                        + ", character1"
                        + ", boolean0"
                        + ", boolean1"
                        + ", byte0"
                        + ", byte1"
                        + ", short0"
                        + ", short1"
                        + ", int0"
                        + ", int1"
                        + ", long0"
                        + ", long1"
                        + ", bigDecimal"
                        + ", bigInteger"
                        + ", float0"
                        + ", float1"
                        + ", double0, double1"
                        + ", \"localTime\""
                        + ", localDate"
                        + ", localDateTime"
                        + ", \"date\""
                        + ", calendar"
                        + ", instant"
                        + ", zonedDateTime"
                        + ", offsetDateTime"
                        + ") VALUES ("
                        + "1"
                        + ", 'string'"
                        + ", 'a'"
                        + ", 'b'"
                        + ", true"
                        + ", false"
                        + ", 126"
                        + ", 127"
                        + ", 32766"
                        + ", 32767"
                        + ", 2147483646"
                        + ", 2147483647"
                        + ", 9223372036854775806"
                        + ", 9223372036854775807"
                        + ", 9223372036854775.123"
                        + ", 9223372036854775222"
                        + ", 1234567890.1"
                        + ", 1234567890.2"
                        + ", 123451234567890.1"
                        + ", 123451234567890.2"
                        + ", time'12:23:34'"
                        + ", date'2020-04-15'"
                        + ", timestamp'2020-04-15 12:23:34.1'"
                        + ", timestamp'2020-04-15 12:23:34.2'"
                        + ", timestamp'2020-04-15 12:23:34.3'"
                        + ", timestamp'2020-04-15 12:23:34.4'"
                        + ", timestamp'2020-04-15 12:23:34.5'"
                        + ", timestamp'2020-04-15 12:23:34.6'"
                        + ")",
                createMap(BigInteger.valueOf(1), new AllTypesValue(
                        "string",
                        'a',
                        'b',
                        true,
                        false,
                        (byte) 126,
                        (byte) 127,
                        (short) 32766,
                        (short) 32767,
                        2147483646,
                        2147483647,
                        9223372036854775806L,
                        9223372036854775807L,
                        new BigDecimal("9223372036854775.123"),
                        new BigInteger("9223372036854775222"),
                        1234567890.1f,
                        1234567890.2f,
                        123451234567890.1,
                        123451234567890.2,
                        LocalTime.of(12, 23, 34),
                        LocalDate.of(2020, 4, 15),
                        // TODO: should be LocalDateTime.of(2020, 4, 15, 12, 23, 34, 100_000_000)
                        //  when temporal types are fixed
                        LocalDateTime.of(2020, 4, 15, 12, 23, 34, 0),
                        Date.from(ofEpochMilli(1586953414200L)),
                        GregorianCalendar.from(ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 300_000_000, UTC)
                                                            .withZoneSameInstant(localOffset())),
                        ofEpochMilli(1586953414400L),
                        ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 500_000_000, UTC)
                                     .withZoneSameInstant(localOffset()),
                        ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 600_000_000, UTC)
                                     .withZoneSameInstant(systemDefault())
                                     .toOffsetDateTime()
                )));

        assertRowsEventuallyInAnyOrder(
                "SELECT"
                        + " __key"
                        + ", string"
                        + ", character0"
                        + ", character1"
                        + ", boolean0"
                        + ", boolean1"
                        + ", byte0, byte1"
                        + ", short0, short1"
                        + ", int0, int1"
                        + ", long0, long1"
                        + ", bigDecimal"
                        + ", bigInteger"
                        + ", float0"
                        + ", float1"
                        + ", double0"
                        + ", double1"
                        + ", \"localTime\""
                        + ", localDate"
                        + ", localDateTime"
                        + ", \"date\""
                        + ", calendar"
                        + ", instant"
                        + ", zonedDateTime"
                        + ", offsetDateTime "
                        + "FROM " + name,
                singletonList(new Row(
                        BigDecimal.valueOf(1),
                        "string",
                        "a",
                        "b",
                        true,
                        false,
                        (byte) 126,
                        (byte) 127,
                        (short) 32766,
                        (short) 32767,
                        2147483646,
                        2147483647,
                        9223372036854775806L,
                        9223372036854775807L,
                        new BigDecimal("9223372036854775.123"),
                        new BigDecimal("9223372036854775222"),
                        1234567890.1f,
                        1234567890.2f,
                        123451234567890.1,
                        123451234567890.2,
                        LocalTime.of(12, 23, 34),
                        LocalDate.of(2020, 4, 15),
                        // TODO: should be LocalDateTime.of(2020, 4, 15, 12, 23, 34, 100_000_000)
                        //  when temporal types are fixed
                        LocalDateTime.of(2020, 4, 15, 12, 23, 34, 0),
                        OffsetDateTime.ofInstant(Date.from(ofEpochMilli(1586953414200L)).toInstant(), systemDefault()),
                        ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 300_000_000, UTC)
                                     .withZoneSameInstant(localOffset())
                                     .toOffsetDateTime(),
                        OffsetDateTime.ofInstant(ofEpochMilli(1586953414400L), systemDefault()),
                        ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 500_000_000, UTC)
                                     .withZoneSameInstant(localOffset())
                                     .toOffsetDateTime(),
                        ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 600_000_000, UTC)
                                     .withZoneSameInstant(systemDefault())
                                     .toOffsetDateTime()
                )));
    }

    @Test
    public void test_mapNameAndTableNameDifferent() {
        String mapName = generateRandomName();
        String tableName = generateRandomName();

        sqlService.execute("CREATE MAPPING " + tableName + " TYPE \"" + IMapSqlConnector.TYPE_NAME + "\"\n"
                + "OPTIONS (\n"
                + '"' + OPTION_OBJECT_NAME + "\" '" + mapName + "',\n"
                + '"' + OPTION_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "',\n"
                + '"' + OPTION_KEY_CLASS + "\" '" + String.class.getName() + "',\n"
                + '"' + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "',\n"
                + '"' + OPTION_VALUE_CLASS + "\" '" + String.class.getName() + "'\n"
                + ")");

        IMap<String, String> map = instance().getMap(mapName);
        map.put("k1", "v1");
        map.put("k2", "v2");

        List<Entry<String, String>> actual = new ArrayList<>();
        for (SqlRow r : sqlService.execute("select * from " + tableName)) {
            actual.add(Util.entry(r.getObject(0), r.getObject(1)));
        }

        assertThat(actual).containsExactlyInAnyOrder(map.entrySet().stream().toArray(Entry[]::new));
    }

    @Test
    public void when_typeMismatch_then_fail() {
        instance().getMap("map").put(0, 0);
        sqlService.execute(javaSerializableMapDdl("map", String.class, String.class));

        assertThatThrownBy(() -> {
            for (SqlRow r : sqlService.execute("select __key from map")) {
                System.out.println(r);
            }
        })
                .hasMessageContaining("Failed to extract map entry key because of type mismatch " +
                        "[expectedClass=java.lang.String, actualClass=java.lang.Integer]");
    }

    protected static ZoneOffset localOffset() {
        return systemDefault().getRules().getOffset(LocalDateTime.now());
    }

    private static String createMapWithRandomName() {
        String name = generateRandomName();
        sqlService.execute(javaSerializableMapDdl(name, PersonId.class, Person.class));
        return name;
    }

    private static String generateRandomName() {
        return "pojo_" + randomString().replace('-', '_');
    }
}
