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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.SqlTestSupport;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.Map;

import static com.hazelcast.jet.sql.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.SqlConnector.JSON_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.TO_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.TO_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.TO_SERIALIZATION_VALUE_FORMAT;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneId.systemDefault;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

// TODO: move it to IMDG when INSERTs are supported, or at least move to one of Jet connector tests ?
public class SqlJsonTest extends SqlTestSupport {

    @Test
    public void supportsNulls() {
        String name = generateRandomName();
        executeSql("CREATE EXTERNAL TABLE " + name + " ("
                + "key_name VARCHAR EXTERNAL NAME \"__key.name\""
                + ", value_name VARCHAR EXTERNAL NAME \"this.name\""
                + ") TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + TO_SERIALIZATION_KEY_FORMAT + "\" '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", \"" + TO_SERIALIZATION_VALUE_FORMAT + "\" '" + JSON_SERIALIZATION_FORMAT + "'"
                + ")");

        executeSql("INSERT OVERWRITE " + name + " VALUES (null, null)");

        Map<HazelcastJsonValue, HazelcastJsonValue> map = instance().getMap(name);
        assertThat(map.get(new HazelcastJsonValue("{\"name\":null}")).toString()).isEqualTo("{\"name\":null}");

        assertRowsEventuallyAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(null, null))
        );
    }

    @Test
    public void supportsFieldsMapping() {
        String name = generateRandomName();
        executeSql("CREATE EXTERNAL TABLE " + name + " ("
                + "key_name VARCHAR EXTERNAL NAME \"__key.name\""
                + ", value_name VARCHAR EXTERNAL NAME \"this.name\""
                + ") TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + TO_SERIALIZATION_KEY_FORMAT + "\" '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", \"" + TO_SERIALIZATION_VALUE_FORMAT + "\" '" + JSON_SERIALIZATION_FORMAT + "'"
                + ")");

        executeSql("INSERT OVERWRITE " + name + " (value_name, key_name) VALUES ('Bob', 'Alice')");

        Map<HazelcastJsonValue, HazelcastJsonValue> map = instance().getMap(name);
        assertThat(map.get(new HazelcastJsonValue("{\"name\":\"Alice\"}")).toString()).isEqualTo("{\"name\":\"Bob\"}");

        assertRowsEventuallyAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row("Alice", "Bob"))
        );
    }

    @Test
    public void supportsSchemaEvolution() {
        String name = generateRandomName();
        executeSql("CREATE EXTERNAL TABLE " + name + " ("
                + "name VARCHAR"
                + ") TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + TO_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + TO_KEY_CLASS + "\" '" + Integer.class.getName() + "', "
                + "\"" + TO_SERIALIZATION_VALUE_FORMAT + "\" '" + JSON_SERIALIZATION_FORMAT + "'"
                + ")");

        // insert initial record
        executeSql("INSERT OVERWRITE " + name + " VALUES (13, 'Alice')");

        // alter schema
        executeSql("CREATE OR REPLACE EXTERNAL TABLE " + name + " ("
                + "name VARCHAR"
                + ", ssn BIGINT"
                + ") TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + TO_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + TO_KEY_CLASS + "\" '" + Integer.class.getName() + "'"
                + ", \"" + TO_SERIALIZATION_VALUE_FORMAT + "\" '" + JSON_SERIALIZATION_FORMAT + "'"
                + ")");

        // insert record against new schema
        executeSql("INSERT OVERWRITE " + name + " VALUES (69, 'Bob', 123456789)");

        // assert both - initial & evolved - records are correctly read
        assertRowsEventuallyAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row(13, "Alice", null),
                        new Row(69, "Bob", 123456789L)
                )
        );
    }

    @Test
    public void supportsAllTypes() {
        String name = generateRandomName();
        executeSql("CREATE EXTERNAL TABLE " + name + " ("
                + "string VARCHAR"
                + ", \"boolean\" BOOLEAN"
                + ", byte TINYINT"
                + ", short SMALLINT"
                + ", \"int\" INT"
                + ", long BIGINT"
                + ", \"float\" REAL"
                + ", \"double\" DOUBLE"
                + ", \"decimal\" DECIMAL"
                + ", \"time\" TIME"
                + ", \"date\" DATE"
                + ", \"timestamp\" TIMESTAMP"
                + ", offsetDateTime TIMESTAMP WITH TIME ZONE"
                + ") TYPE \"" + LocalPartitionedMapConnector.TYPE_NAME + "\" "
                + "OPTIONS ("
                + "\"" + TO_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + TO_KEY_CLASS + "\" '" + BigInteger.class.getName() + "'"
                + ", \"" + TO_SERIALIZATION_VALUE_FORMAT + "\" '" + JSON_SERIALIZATION_FORMAT + "'"
                + ")");

        executeSql("INSERT OVERWRITE " + name + " VALUES ("
                + "1"
                + ", 'string'"
                + ", true"
                + ", 126"
                + ", 32766"
                + ", 2147483646"
                + ", 9223372036854775806"
                + ", 1234567890.1"
                + ", 123451234567890.1"
                + ", 9223372036854775.123"
                + ", time'12:23:34'"
                + ", date'2020-04-15'"
                + ", timestamp'2020-04-15 12:23:34.1'"
                + ", timestamp'2020-04-15 12:23:34.2'"
                + ")");

        assertRowsEventuallyAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(
                        BigDecimal.valueOf(1),
                        "string",
                        true,
                        (byte) 126,
                        (short) 32766,
                        2147483646,
                        9223372036854775806L,
                        1234567890.1f,
                        123451234567890.1,
                        new BigDecimal("9223372036854775.123"),
                        LocalTime.of(12, 23, 34),
                        LocalDate.of(2020, 4, 15),
                        // TODO: should be LocalDateTime.of(2020, 4, 15, 12, 23, 34, 100_000_000)
                        //  when temporal types are fixed
                        LocalDateTime.of(2020, 4, 15, 12, 23, 34, 0),
                        OffsetDateTime.ofInstant(Date.from(ofEpochMilli(1586953414200L)).toInstant(), systemDefault())
                ))
        );
    }

    private static String generateRandomName() {
        return "json_" + randomString().replace('-', '_');
    }
}
