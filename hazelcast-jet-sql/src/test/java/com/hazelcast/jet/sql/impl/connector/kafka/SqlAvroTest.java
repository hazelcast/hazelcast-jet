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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.AllTypesSqlConnector;
import com.hazelcast.sql.SqlService;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecordBuilder;
import org.eclipse.jetty.server.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class SqlAvroTest extends SqlTestSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;

    private static KafkaTestSupport kafkaTestSupport;
    private static Server schemaRegistry;

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() throws Exception {
        initialize(1, null);
        sqlService = instance().getSql();

        kafkaTestSupport = new KafkaTestSupport();
        kafkaTestSupport.createKafkaCluster();

        Properties properties = new Properties();
        properties.put("listeners", "http://0.0.0.0:" + randomPort());
        properties.put("kafkastore.connection.url", kafkaTestSupport.getZookeeperConnectionString());
        SchemaRegistryConfig config = new SchemaRegistryConfig(properties);
        SchemaRegistryRestApplication schemaRegistryApplication = new SchemaRegistryRestApplication(config);
        schemaRegistry = schemaRegistryApplication.createServer();
        schemaRegistry.start();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        schemaRegistry.stop();
        kafkaTestSupport.shutdownKafkaCluster();
    }

    @Test
    public void test_nulls() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME \"__key.id\""
                + ", name VARCHAR EXTERNAL NAME \"this.name\""
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '"' + OPTION_KEY_FORMAT + "\" '" + AVRO_FORMAT + '\''
                + ", \"" + OPTION_VALUE_FORMAT + "\" '" + AVRO_FORMAT + '\''
                + ", \"bootstrap.servers\" '" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", \"schema.registry.url\" '" + schemaRegistry.getURI() + '\''
                + ", \"key.serializer\" '" + KafkaAvroSerializer.class.getCanonicalName() + '\''
                + ", \"key.deserializer\" '" + KafkaAvroDeserializer.class.getCanonicalName() + '\''
                + ", \"value.serializer\" '" + KafkaAvroSerializer.class.getCanonicalName() + '\''
                + ", \"value.deserializer\" '" + KafkaAvroDeserializer.class.getCanonicalName() + '\''
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " VALUES (null, null)",
                createMap(
                        new GenericRecordBuilder(intSchema("id")).build(),
                        new GenericRecordBuilder(stringSchema("name")).set("name", null).build()
                )
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
                + "key_name VARCHAR EXTERNAL NAME \"__key.name\""
                + ", value_name VARCHAR EXTERNAL NAME \"this.name\""
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '"' + OPTION_KEY_FORMAT + "\" '" + AVRO_FORMAT + '\''
                + ", \"" + OPTION_VALUE_FORMAT + "\" '" + AVRO_FORMAT + '\''
                + ", \"bootstrap.servers\" '" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", \"schema.registry.url\" '" + schemaRegistry.getURI() + '\''
                + ", \"key.serializer\" '" + KafkaAvroSerializer.class.getCanonicalName() + '\''
                + ", \"key.deserializer\" '" + KafkaAvroDeserializer.class.getCanonicalName() + '\''
                + ", \"value.serializer\" '" + KafkaAvroSerializer.class.getCanonicalName() + '\''
                + ", \"value.deserializer\" '" + KafkaAvroDeserializer.class.getCanonicalName() + '\''
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (value_name, key_name) VALUES ('Bob', 'Alice')",
                createMap(
                        new GenericRecordBuilder(stringSchema("name")).set("name", "Alice").build(),
                        new GenericRecordBuilder(stringSchema("name")).set("name", "Bob").build()
                )
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
                + "id INT EXTERNAL NAME \"__key.id\""
                + ", name VARCHAR"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '"' + OPTION_KEY_FORMAT + "\" '" + AVRO_FORMAT + '\''
                + ", \"" + OPTION_VALUE_FORMAT + "\" '" + AVRO_FORMAT + '\''
                + ", \"bootstrap.servers\" '" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", \"schema.registry.url\" '" + schemaRegistry.getURI() + '\''
                + ", \"key.serializer\" '" + KafkaAvroSerializer.class.getCanonicalName() + '\''
                + ", \"key.deserializer\" '" + KafkaAvroDeserializer.class.getCanonicalName() + '\''
                + ", \"value.serializer\" '" + KafkaAvroSerializer.class.getCanonicalName() + '\''
                + ", \"value.deserializer\" '" + KafkaAvroDeserializer.class.getCanonicalName() + '\''
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        // insert initial record
        sqlService.execute("INSERT INTO " + name + " VALUES (13, 'Alice')");

        // alter schema
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME \"__key.id\""
                + ", name VARCHAR"
                + ", ssn BIGINT"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '"' + OPTION_KEY_FORMAT + "\" '" + AVRO_FORMAT + '\''
                + ", \"" + OPTION_VALUE_FORMAT + "\" '" + AVRO_FORMAT + '\''
                + ", \"bootstrap.servers\" '" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", \"schema.registry.url\" '" + schemaRegistry.getURI() + '\''
                + ", \"key.serializer\" '" + KafkaAvroSerializer.class.getCanonicalName() + '\''
                + ", \"key.deserializer\" '" + KafkaAvroDeserializer.class.getCanonicalName() + '\''
                + ", \"value.serializer\" '" + KafkaAvroSerializer.class.getCanonicalName() + '\''
                + ", \"value.deserializer\" '" + KafkaAvroDeserializer.class.getCanonicalName() + '\''
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
    @SuppressWarnings("checkstyle:LineLength")
    public void test_allTypes() {
        String from = generateRandomName();
        AllTypesSqlConnector.create(sqlService, from);

        String to = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + to + " ("
                + "id VARCHAR EXTERNAL NAME \"__key.id\""
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
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '"' + OPTION_KEY_FORMAT + "\" '" + AVRO_FORMAT + '\''
                + ", \"" + OPTION_VALUE_FORMAT + "\" '" + AVRO_FORMAT + '\''
                + ", \"bootstrap.servers\" '" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", \"schema.registry.url\" '" + schemaRegistry.getURI() + '\''
                + ", \"key.serializer\" '" + KafkaAvroSerializer.class.getCanonicalName() + '\''
                + ", \"key.deserializer\" '" + KafkaAvroDeserializer.class.getCanonicalName() + '\''
                + ", \"value.serializer\" '" + KafkaAvroSerializer.class.getCanonicalName() + '\''
                + ", \"value.deserializer\" '" + KafkaAvroDeserializer.class.getCanonicalName() + '\''
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        sqlService.execute("INSERT INTO " + to + " SELECT 1, f.* FROM " + from + " f");

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
                        ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC).withZoneSameInstant(systemDefault()).toOffsetDateTime()
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

    private static void assertTopicEventually(String name, String sql, Map<Object, Object> expected) {
        sqlService.execute(sql);

        kafkaTestSupport.assertTopicContentsEventually(
                name,
                expected,
                KafkaAvroDeserializer.class,
                KafkaAvroDeserializer.class,
                ImmutableMap.of("schema.registry.url", schemaRegistry.getURI().toString())
        );
    }

    private static Schema intSchema(String fieldName) {
        return SchemaBuilder.record("jet.sql")
                            .fields()
                            .name(fieldName).type().unionOf().nullType().and().intType().endUnion().nullDefault()
                            .endRecord();
    }

    private static Schema stringSchema(String fieldName) {
        return SchemaBuilder.record("jet.sql")
                            .fields()
                            .name(fieldName).type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                            .endRecord();
    }

    private static int randomPort() throws IOException {
        try (ServerSocket server = new ServerSocket(0)) {
            return server.getLocalPort();
        }
    }
}
