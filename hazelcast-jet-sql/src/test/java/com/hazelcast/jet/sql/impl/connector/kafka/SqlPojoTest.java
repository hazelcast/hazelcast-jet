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
import com.hazelcast.jet.sql.impl.connector.kafka.model.AllCanonicalTypesValueDeserializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.AllCanonicalTypesValueSerializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.Person;
import com.hazelcast.jet.sql.impl.connector.kafka.model.PersonDeserializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.PersonId;
import com.hazelcast.jet.sql.impl.connector.kafka.model.PersonIdDeserializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.PersonIdSerializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.PersonSerializer;
import com.hazelcast.jet.sql.impl.connector.test.AllTypesSqlConnector;
import com.hazelcast.sql.SqlService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Map;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.singletonList;

public class SqlPojoTest extends JetSqlTestSupport {

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

    @AfterClass
    public static void tearDownClass() {
        kafkaTestSupport.shutdownKafkaCluster();
    }

    @Test
    public void test_nulls() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '"' + OPTION_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + '\''
                + ", \"" + OPTION_KEY_CLASS + "\" '" + PersonId.class.getName() + '\''
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + '\''
                + ", \"" + OPTION_VALUE_CLASS + "\" '" + Person.class.getName() + '\''
                + ", \"bootstrap.servers\" '" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", \"key.serializer\" '" + PersonIdSerializer.class.getCanonicalName() + '\''
                + ", \"key.deserializer\" '" + PersonIdDeserializer.class.getCanonicalName() + '\''
                + ", \"value.serializer\" '" + PersonSerializer.class.getCanonicalName() + '\''
                + ", \"value.deserializer\" '" + PersonDeserializer.class.getCanonicalName() + '\''
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " VALUES (null, null)",
                createMap(new PersonId(), new Person())
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(null, null))
        );
    }

    @Test
    public void test_fieldsShadowing() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '"' + OPTION_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + '\''
                + ", \"" + OPTION_KEY_CLASS + "\" '" + PersonId.class.getName() + '\''
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + '\''
                + ", \"" + OPTION_VALUE_CLASS + "\" '" + Person.class.getName() + '\''
                + ", \"bootstrap.servers\" '" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", \"key.serializer\" '" + PersonIdSerializer.class.getCanonicalName() + '\''
                + ", \"key.deserializer\" '" + PersonIdDeserializer.class.getCanonicalName() + '\''
                + ", \"value.serializer\" '" + PersonSerializer.class.getCanonicalName() + '\''
                + ", \"value.deserializer\" '" + PersonDeserializer.class.getCanonicalName() + '\''
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " VALUES (1, 'Alice')",
                createMap(new PersonId(1), new Person(null, "Alice"))
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "Alice"))
        );
    }

    @Test
    public void test_fieldsMapping() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "key_id INT EXTERNAL NAME \"__key.id\""
                + ", value_id INT EXTERNAL NAME \"this.id\""
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '"' + OPTION_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + '\''
                + ", \"" + OPTION_KEY_CLASS + "\" '" + PersonId.class.getName() + '\''
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + '\''
                + ", \"" + OPTION_VALUE_CLASS + "\" '" + Person.class.getName() + '\''
                + ", \"bootstrap.servers\" '" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", \"key.serializer\" '" + PersonIdSerializer.class.getCanonicalName() + '\''
                + ", \"key.deserializer\" '" + PersonIdDeserializer.class.getCanonicalName() + '\''
                + ", \"value.serializer\" '" + PersonSerializer.class.getCanonicalName() + '\''
                + ", \"value.deserializer\" '" + PersonDeserializer.class.getCanonicalName() + '\''
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (value_id, key_id, name) VALUES (2, 1, 'Alice')",
                createMap(new PersonId(1), new Person(2, "Alice"))
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT  key_id, value_id, name FROM " + name,
                singletonList(new Row(1, 2, "Alice"))
        );
    }

    @Test
    @SuppressWarnings("checkstyle:LineLength")
    public void test_allTypes() {
        String from = generateRandomName();
        AllTypesSqlConnector.create(sqlService, from);

        String to = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + to + ' '
                + " TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '"' + OPTION_SERIALIZATION_KEY_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + '\''
                + ", \"" + OPTION_KEY_CLASS + "\" '" + PersonId.class.getName() + '\''
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + '\''
                + ", \"" + OPTION_VALUE_CLASS + "\" '" + AllCanonicalTypesValue.class.getName() + '\''
                + ", \"bootstrap.servers\" '" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", \"key.serializer\" '" + PersonIdSerializer.class.getCanonicalName() + '\''
                + ", \"key.deserializer\" '" + PersonIdDeserializer.class.getCanonicalName() + '\''
                + ", \"value.serializer\" '" + AllCanonicalTypesValueSerializer.class.getCanonicalName() + '\''
                + ", \"value.deserializer\" '" + AllCanonicalTypesValueDeserializer.class.getCanonicalName() + '\''
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        sqlService.execute("INSERT INTO " + to + "("
                + "id"
                + ", string"
                + ", boolean0"
                + ", byte0"
                + ", short0"
                + ", int0"
                + ", long0"
                + ", float0"
                + ", double0"
                + ", \"decimal\""
                + ", \"time\""
                + ", \"date\""
                + ", \"timestamp\""
                + ", timestampTz"
                + ") SELECT "
                + "CAST(1 AS INT)"
                + ", string"
                + ", \"boolean\""
                + ", byte"
                + ", short"
                + ", \"int\""
                + ", long"
                + ", \"float\""
                + ", \"double\""
                + ", \"decimal\""
                + ", \"time\""
                + ", \"date\""
                + ", \"timestamp\""
                + ", \"timestampTz\""
                + " FROM " + from
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT "
                        + "id"
                        + ", string"
                        + ", boolean0"
                        + ", byte0"
                        + ", short0"
                        + ", int0"
                        + ", long0"
                        + ", float0"
                        + ", double0"
                        + ", \"decimal\""
                        + ", \"time\""
                        + ", \"date\""
                        + ", \"timestamp\""
                        + ", timestampTz"
                        + " FROM " + to,
                singletonList(new Row(
                        1,
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
                        ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC).withZoneSameInstant(systemDefault()).toOffsetDateTime()
                ))
        );
    }

    private static String generateRandomName() {
        return "pojo_" + randomString().replace('-', '_');
    }

    private static String createRandomTopic() {
        String topicName = "t_" + randomString().replace('-', '_');
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        return topicName;
    }

    private static void assertTopicEventually(String name, String sql, Map<PersonId, Person> expected) {
        sqlService.execute(sql);

        kafkaTestSupport.assertTopicContentsEventually(
                name,
                expected,
                PersonIdDeserializer.class,
                PersonDeserializer.class
        );
    }
}
