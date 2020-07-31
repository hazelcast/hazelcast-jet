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

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.model.AllTypesValue;
import com.hazelcast.jet.sql.impl.connector.map.model.InsuredPerson;
import com.hazelcast.jet.sql.impl.connector.map.model.Person;
import com.hazelcast.jet.sql.impl.connector.map.model.PersonId;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.TO_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.TO_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.TO_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.TO_VALUE_CLASS;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

// TODO: move it to IMDG when INSERTs are supported, or at least move to one of Jet connector tests ?
public class SqlPojoTest extends SqlTestSupport {

    @Test
    public void supportsNulls() {
        String name = createMapWithRandomName();

        assertMapEventually(
                name,
                "INSERT OVERWRITE " + name + " VALUES (null, null)",
                createMap(new PersonId(), new Person()));

        assertRowsEventuallyAnyOrder(
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

        assertRowsEventuallyAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "Alice"))
        );
    }

    @Test
    public void supportsFieldsMapping() {
        String name = generateRandomName();
        executeSql("CREATE EXTERNAL TABLE " + name + " ("
                + "key_id INT EXTERNAL NAME \"__key.id\""
                + ", value_id INT EXTERNAL NAME \"this.id\""
                + ") TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + TO_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + TO_KEY_CLASS + "\" '" + PersonId.class.getName() + "'"
                + ", \"" + TO_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + TO_VALUE_CLASS + "\" '" + Person.class.getName() + "'"
                + ")"
        );

        assertMapEventually(
                name,
                "INSERT OVERWRITE " + name + " (value_id, key_id, name) VALUES (2, 1, 'Alice')",
                createMap(new PersonId(1), new Person(2, "Alice"))
        );

        assertRowsEventuallyAnyOrder(
                "SELECT key_id, value_id, name FROM " + name,
                singletonList(new Row(1, 2, "Alice"))
        );
    }

    @Test
    public void supportsSchemaEvolution() {
        String name = createMapWithRandomName();

        // insert initial record
        executeSql("INSERT OVERWRITE " + name + " VALUES (1, 'Alice')");

        // alter schema
        executeSql("CREATE OR REPLACE EXTERNAL TABLE " + name + " "
                + "TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + TO_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + TO_KEY_CLASS + "\" '" + PersonId.class.getName() + "'"
                + ", \"" + TO_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + TO_VALUE_CLASS + "\" '" + InsuredPerson.class.getName() + "'"
                + ")"
        );

        // insert record against new schema
        executeSql("INSERT OVERWRITE " + name + " (id, name, ssn) VALUES (2, 'Bob', 123456789)");

        // assert both - initial & evolved - records are correctly read
        assertRowsEventuallyAnyOrder(
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

        executeSql("CREATE EXTERNAL TABLE " + name + " ("
                + "ssn BIGINT"
                + ") TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + TO_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + TO_KEY_CLASS + "\" '" + PersonId.class.getName() + "'"
                + ", \"" + TO_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + TO_VALUE_CLASS + "\" '" + Person.class.getName() + "'"
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

        assertRowsEventuallyAnyOrder(
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
        executeSql("CREATE EXTERNAL TABLE " + name + " "
                + "TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + TO_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + TO_KEY_CLASS + "\" '" + BigInteger.class.getName() + "'"
                + ", \"" + TO_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + TO_VALUE_CLASS + "\" '" + AllTypesValue.class.getName() + "'"
                + ")"
        );

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

        assertRowsEventuallyAnyOrder(
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

    private static String createMapWithRandomName() {
        String name = generateRandomName();
        executeSql("CREATE EXTERNAL TABLE " + name + " "
                + "TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + TO_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + TO_KEY_CLASS + "\" '" + PersonId.class.getName() + "'"
                + ", \"" + TO_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + TO_VALUE_CLASS + "\" '" + Person.class.getName() + "'"
                + ")"
        );
        return name;
    }

    private static String generateRandomName() {
        return "pojo_" + randomString().replace('-', '_');
    }
}
