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

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.JetSqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.kafka.model.AllCanonicalTypesValue;
import com.hazelcast.sql.SqlService;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Map;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_VALUE_FORMAT;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class SqlJsonTest extends JetSqlTestSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;

    private static KafkaTestSupport kafkaTestSupport;

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() throws IOException {
        initialize(1, null);
        sqlService = instance().getSql();

        kafkaTestSupport = new KafkaTestSupport();
        kafkaTestSupport.createKafkaCluster();
    }

    @Test
    public void test_nulls() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME __key.id"
                + ", name VARCHAR EXTERNAL NAME this.name"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + " "
                + "OPTIONS ( "
                + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", bootstrap.servers '" + kafkaTestSupport.getBrokerConnectionString() + "'"
                + ", key.serializer '" + JsonSerializer.class.getCanonicalName() + "'"
                + ", key.deserializer '" + JsonDeserializer.class.getCanonicalName() + "'"
                + ", \"value.serializer\" '" + JsonSerializer.class.getCanonicalName() + "'"
                + ", \"value.deserializer\" '" + JsonDeserializer.class.getCanonicalName() + "'"
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " VALUES (null, null)",
                createMap("{\"id\":null}", "{\"name\":null}")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(null, null))
        );
    }

    @Test
    public void test_fieldsMapping() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "key_name VARCHAR EXTERNAL NAME __key.name"
                + ", value_name VARCHAR EXTERNAL NAME this.name"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + " "
                + "OPTIONS ( "
                + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", bootstrap.servers '" + kafkaTestSupport.getBrokerConnectionString() + "'"
                + ", key.serializer '" + JsonSerializer.class.getCanonicalName() + "'"
                + ", key.deserializer '" + JsonDeserializer.class.getCanonicalName() + "'"
                + ", \"value.serializer\" '" + JsonSerializer.class.getCanonicalName() + "'"
                + ", \"value.deserializer\" '" + JsonDeserializer.class.getCanonicalName() + "'"
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (value_name, key_name) VALUES ('Bob', 'Alice')",
                createMap("{\"name\":\"Alice\"}", "{\"name\":\"Bob\"}")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row("Alice", "Bob"))
        );
    }

    @Test
    public void test_schemaEvolution() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME __key.id"
                + ", name VARCHAR"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + " "
                + "OPTIONS ( "
                + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", bootstrap.servers '" + kafkaTestSupport.getBrokerConnectionString() + "'"
                + ", key.serializer '" + JsonSerializer.class.getCanonicalName() + "'"
                + ", key.deserializer '" + JsonDeserializer.class.getCanonicalName() + "'"
                + ", \"value.serializer\" '" + JsonSerializer.class.getCanonicalName() + "'"
                + ", \"value.deserializer\" '" + JsonDeserializer.class.getCanonicalName() + "'"
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        // insert initial record
        sqlService.execute("INSERT INTO " + name + " VALUES (13, 'Alice')");

        // alter schema
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME __key.id"
                + ", name VARCHAR"
                + ", ssn BIGINT"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + " "
                + "OPTIONS ( "
                + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", bootstrap.servers '" + kafkaTestSupport.getBrokerConnectionString() + "'"
                + ", key.serializer '" + JsonSerializer.class.getCanonicalName() + "'"
                + ", key.deserializer '" + JsonDeserializer.class.getCanonicalName() + "'"
                + ", \"value.serializer\" '" + JsonSerializer.class.getCanonicalName() + "'"
                + ", \"value.deserializer\" '" + JsonDeserializer.class.getCanonicalName() + "'"
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        // insert record against new schema
        sqlService.execute("INSERT INTO " + name + " VALUES (69, 'Bob', 123456789)");

        // assert both - initial & evolved - records are correctly read
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row(13, "Alice", null),
                        new Row(69, "Bob", 123456789L)
                )
        );
    }

    @Test
    public void test_allTypes() {
        String from = generateRandomName();
        instance().getMap(from).put(1, new AllCanonicalTypesValue(
                "string",
                true,
                (byte) 127,
                (short) 32767,
                2147483647,
                9223372036854775807L,
                new BigDecimal("9223372036854775.123"),
                1234567890.1f,
                123451234567890.1,
                LocalTime.of(12, 23, 34),
                LocalDate.of(2020, 4, 15),
                LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000),
                ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC)
                             .withZoneSameInstant(systemDefault())
                             .toOffsetDateTime()
        ));

        String to = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + to + " ("
                + "id VARCHAR EXTERNAL NAME __key.id"
                + ", string VARCHAR"
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
                + ", timestampTz TIMESTAMP WITH TIME ZONE"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + " "
                + "OPTIONS ( "
                + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JSON_SERIALIZATION_FORMAT + "'"
                + ", bootstrap.servers '" + kafkaTestSupport.getBrokerConnectionString() + "'"
                + ", key.serializer '" + JsonSerializer.class.getCanonicalName() + "'"
                + ", key.deserializer '" + JsonDeserializer.class.getCanonicalName() + "'"
                + ", \"value.serializer\" '" + JsonSerializer.class.getCanonicalName() + "'"
                + ", \"value.deserializer\" '" + JsonDeserializer.class.getCanonicalName() + "'"
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        sqlService.execute("INSERT INTO " + to + " SELECT "
                + "__key"
                + ", string "
                + ", boolean0 "
                + ", byte0 "
                + ", short0 "
                + ", int0 "
                + ", long0 "
                + ", float0 "
                + ", double0 "
                + ", bigDecimal "
                + ", \"localTime\" "
                + ", \"localDate\" "
                + ", \"localDateTime\" "
                + ", offsetDateTime"
                + " FROM " + from);

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + to,
                singletonList(new Row(
                        "1",
                        "string",
                        true,
                        (byte) 127,
                        (short) 32767,
                        2147483647,
                        9223372036854775807L,
                        1234567890.1f,
                        123451234567890.1,
                        new BigDecimal("9223372036854775.123"),
                        LocalTime.of(12, 23, 34),
                        LocalDate.of(2020, 4, 15),
                        LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000),
                        OffsetDateTime.ofInstant(Date.from(ofEpochMilli(1586953414200L)).toInstant(), systemDefault())
                ))
        );
    }

    private static String generateRandomName() {
        return "json_" + randomString().replace('-', '_');
    }

    private static String createRandomTopic() {
        String topicName = "t_" + randomString().replace('-', '_');
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        return topicName;
    }

    private static void assertTopicEventually(String name, String sql, Map<String, String> expected) {
        sqlService.execute(sql);

        kafkaTestSupport.assertTopicContentsEventually(
                name,
                expected,
                StringDeserializer.class,
                StringDeserializer.class
        );
    }
}
