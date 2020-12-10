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

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.google.common.collect.ImmutableMap;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.AllTypesSqlConnector;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlService;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class SqlAvroTest extends SqlTestSupport {

    private static Network network;
    private static ZookeeperContainer zookeeper;
    private static KafkaContainer kafka;
    private static SchemaRegistryContainer schemaRegistry;

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() throws Exception {
        initialize(1, null);
        sqlService = instance().getSql();

        network = Network.newNetwork();

        zookeeper = new ZookeeperContainer(network);
        zookeeper.start();

        kafka = new KafkaContainer(network, zookeeper.internalUrl());
        kafka.start();

        schemaRegistry = new SchemaRegistryContainer(network, zookeeper.internalUrl());
        schemaRegistry.start();
    }

    @AfterClass
    public static void tearDownClass() {
        schemaRegistry.stop();
        kafka.stop();
        zookeeper.stop();
        network.close();
    }

    @Test
    public void test_nulls() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME \"__key.id\""
                + ", name VARCHAR"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafka.getBootstrapServers() + '\''
                + ", 'schema.registry.url'='" + schemaRegistry.url() + '\''
                + ", 'auto.offset.reset'='earliest'"
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
                + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafka.getBootstrapServers() + '\''
                + ", 'schema.registry.url'='" + schemaRegistry.url() + '\''
                + ", 'auto.offset.reset'='earliest'"
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
                + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafka.getBootstrapServers() + '\''
                + ", 'schema.registry.url'='" + schemaRegistry.url() + '\''
                + ", 'auto.offset.reset'='earliest'"
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
                + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafka.getBootstrapServers() + '\''
                + ", 'schema.registry.url'='" + schemaRegistry.url() + '\''
                + ", 'auto.offset.reset'='earliest'"
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
        String from = randomName();
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
                + ", object OBJECT"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafka.getBootstrapServers() + '\''
                + ", 'schema.registry.url'='" + schemaRegistry.url() + '\''
                + ", 'auto.offset.reset'='earliest'"
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
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC),
                        null
                ))
        );
    }

    @Test
    public void test_avroPrimitiveValue_key() {
        test_avroPrimitiveValue("__key");
    }

    @Test
    public void test_avroPrimitiveValue_value() {
        test_avroPrimitiveValue("this");
    }

    private void test_avroPrimitiveValue(String path) {
        assertThatThrownBy(
                () -> sqlService.execute("CREATE MAPPING " + randomName() + " ("
                        + path + " INT"
                        + ") TYPE Kafka "
                        + "OPTIONS ("
                        + "'keyFormat'='avro'"
                        + ", 'valueFormat'='avro'"
                        + ")"))
                .hasMessage("Cannot use the '" + path + "' field with Avro serialization");
    }

    @Test
    public void test_schemaIdForTwoQueriesIsEqual() {
        String topicName = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + topicName + " (__key INT, field1 VARCHAR) "
                + "TYPE Kafka "
                + "OPTIONS ("
                + "'keyFormat'='java'"
                + ", 'keyJavaClass'='java.lang.Integer'"
                + ", 'valueFormat'='avro'"
                + ", 'bootstrap.servers'='" + kafka.getBootstrapServers() + '\''
                + ", 'schema.registry.url'='" + schemaRegistry.url() + '\''
                + ", 'auto.offset.reset'='earliest')"
        );

        sqlService.execute("INSERT INTO " + topicName + " VALUES(42, 'foo')");
        sqlService.execute("INSERT INTO " + topicName + " VALUES(43, 'bar')");

        try (KafkaConsumer<Integer, byte[]> consumer = KafkaTestSupport.createConsumer(
                kafka.getBootstrapServers(),
                IntegerDeserializer.class,
                ByteArrayDeserializer.class,
                emptyMap(),
                topicName
        )) {
            long timeLimit = System.nanoTime() + SECONDS.toNanos(10);
            List<Integer> schemaIds = new ArrayList<>();
            while (schemaIds.size() < 2) {
                if (System.nanoTime() > timeLimit) {
                    Assert.fail("Timeout waiting for the records from Kafka");
                }
                ConsumerRecords<Integer, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Integer, byte[]> record : records) {
                    int id = Bits.readInt(record.value(), 0, true);
                    schemaIds.add(id);
                }
            }
            assertEquals("The schemaIds of the two records don't match", schemaIds.get(0), schemaIds.get(1));
        }
    }

    @Test
    public void when_createMappingNoColumns_then_fail() {
        assertThatThrownBy(() ->
                sqlService.execute("CREATE MAPPING " + randomName() + " "
                        + "TYPE Kafka "
                        + "OPTIONS ('valueFormat'='avro')"))
                .hasMessage("Column list is required for Avro format");
    }

    @Test
    public void when_explicitTopLevelField_then_fail_key() {
        when_explicitTopLevelField_then_fail("__key", "this");
    }

    @Test
    public void when_explicitTopLevelField_then_fail_this() {
        when_explicitTopLevelField_then_fail("this", "__key");
    }

    public void when_explicitTopLevelField_then_fail(String field, String otherField) {
        String name = randomName();
        assertThatThrownBy(() ->
                sqlService.execute("CREATE MAPPING " + name + " ("
                        + field + " VARCHAR"
                        + ", f VARCHAR EXTERNAL NAME \"" + otherField + ".f\""
                        + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ("
                        + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                        + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
                        + ")"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessage("Cannot use the '" + field + "' field with Avro serialization");
    }

    @Test
    public void test_topLevelFieldExtraction() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME \"__key.id\""
                + ", name VARCHAR"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafka.getBootstrapServers() + '\''
                + ", 'schema.registry.url'='" + schemaRegistry.url() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );
        sqlService.execute("INSERT INTO " + name + " VALUES (1, 'Alice')");

        assertRowsEventuallyInAnyOrder(
                "SELECT __key, this FROM " + name,
                singletonList(new Row(
                        new GenericRecordBuilder(intSchema("id")).set("id", 1).build(),
                        new GenericRecordBuilder(stringSchema("name")).set("name", "Alice").build()
                ))
        );
    }

    @Test
    public void test_explicitKeyAndValueSerializers() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "key_name VARCHAR EXTERNAL NAME \"__key.name\""
                + ", value_name VARCHAR EXTERNAL NAME \"this.name\""
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", 'key.serializer'='" + KafkaAvroSerializer.class.getCanonicalName() + '\''
                + ", 'key.deserializer'='" + KafkaAvroDeserializer.class.getCanonicalName() + '\''
                + ", 'value.serializer'='" + KafkaAvroSerializer.class.getCanonicalName() + '\''
                + ", 'value.deserializer'='" + KafkaAvroDeserializer.class.getCanonicalName() + '\''
                + ", 'bootstrap.servers'='" + kafka.getBootstrapServers() + '\''
                + ", 'schema.registry.url'='" + schemaRegistry.url() + '\''
                + ", 'auto.offset.reset'='earliest'"
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

    private static String createRandomTopic() {
        return "t_" + randomString().replace('-', '_');
    }

    private void assertTopicEventually(String name, String sql, Map<Object, Object> expected) {
        sqlService.execute(sql);

        KafkaTestSupport.assertTopicContentsEventually(
                kafka.getBootstrapServers(),
                name,
                expected,
                KafkaAvroDeserializer.class,
                KafkaAvroDeserializer.class,
                ImmutableMap.of("schema.registry.url", schemaRegistry.url())
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

    private static final class ZookeeperContainer extends GenericContainer<ZookeeperContainer> {

        private static final int ZOOKEEPER_INTERNAL_PORT = 2181;
        private static final int ZOOKEEPER_TICK_TIME = 2000;
        private static final String NETWORK_ALIAS = "zookeeper";

        public ZookeeperContainer(Network network) {
            super(DockerImageName.parse(("confluentinc/cp-zookeeper:4.1.4")));

            withEnv("ZOOKEEPER_CLIENT_PORT", Integer.toString(ZOOKEEPER_INTERNAL_PORT));
            withEnv("ZOOKEEPER_TICK_TIME", Integer.toString(ZOOKEEPER_TICK_TIME));

            withNetwork(network);
            withNetworkAliases(NETWORK_ALIAS);
            addExposedPort(ZOOKEEPER_INTERNAL_PORT);
        }

        private String internalUrl() {
            return format("%s:%d", NETWORK_ALIAS, ZOOKEEPER_INTERNAL_PORT);
        }
    }

    private static final class KafkaContainer extends GenericContainer<KafkaContainer> {

        private static final String STARTER_SCRIPT = "/testcontainers_start.sh";

        private static final int KAFKA_PORT = 9093;
        private static final int KAFKA_INTERNAL_PORT = 9092;
        private static final int PORT_NOT_ASSIGNED = -1;
        private static final String NETWORK_ALIAS = "kafka";

        private final String zookeeperConnect;

        private int port = PORT_NOT_ASSIGNED;

        public KafkaContainer(Network network, String zookeeperConnect) {
            super(DockerImageName.parse("confluentinc/cp-kafka:4.1.4"));

            this.zookeeperConnect = zookeeperConnect;

            // Use two listeners with different names, it will force Kafka to communicate with itself via internal
            // listener when KAFKA_INTER_BROKER_LISTENER_NAME is set, otherwise Kafka will try to use the advertised listener
            withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:" + KAFKA_PORT + ",BROKER://0.0.0.0:" + KAFKA_INTERNAL_PORT);

            withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
            withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");

            withEnv("KAFKA_BROKER_ID", "1");
            withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
            withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1");
            withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "");
            withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");

            withNetwork(network);
            withNetworkAliases(NETWORK_ALIAS);
            withExposedPorts(KAFKA_INTERNAL_PORT);
        }

        private String getBootstrapServers() {
            if (port == PORT_NOT_ASSIGNED) {
                throw new IllegalStateException("You should start Kafka container first");
            }
            return String.format("PLAINTEXT://%s:%s", getHost(), port);
        }

        @Override
        protected void doStart() {
            withCommand("sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);

            super.doStart();
        }

        @Override
        protected void containerIsStarting(InspectContainerResponse containerInfo, boolean reused) {
            super.containerIsStarting(containerInfo, reused);

            port = getMappedPort(KAFKA_INTERNAL_PORT);

            if (reused) {
                return;
            }


            String command = "#!/bin/bash \n";
            command += "export KAFKA_ZOOKEEPER_CONNECT='" + zookeeperConnect + "'\n";
            command += "export KAFKA_ADVERTISED_LISTENERS='" + Stream
                    .concat(
                            Stream.of("PLAINTEXT://" + NETWORK_ALIAS + ":" + KAFKA_PORT),
                            containerInfo.getNetworkSettings().getNetworks().values().stream()
                                         .map(it -> "BROKER://" + it.getIpAddress() + ":" + KAFKA_INTERNAL_PORT)
                    )
                    .collect(Collectors.joining(",")) + "'\n";

            command += ". /etc/confluent/docker/bash-config \n";
            command += "/etc/confluent/docker/configure \n";
            command += "/etc/confluent/docker/launch \n";

            copyFileToContainer(
                    Transferable.of(command.getBytes(StandardCharsets.UTF_8), 700),
                    STARTER_SCRIPT
            );
        }
    }

    private static final class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

        private static final int SCHEMA_REGISTRY_INTERNAL_PORT = 8081;
        private static final String NETWORK_ALIAS = "schema-registry";

        private SchemaRegistryContainer(Network network, String zookeeperConnect) {
            super(DockerImageName.parse("confluentinc/cp-schema-registry:4.1.4"));

            withEnv("SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL", zookeeperConnect);
            withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost");

            withNetwork(network);
            withNetworkAliases(NETWORK_ALIAS);
            withExposedPorts(SCHEMA_REGISTRY_INTERNAL_PORT);

            waitingFor(Wait.forHttp("/subjects"));
        }

        private String url() {
            return format("http://%s:%d", getContainerIpAddress(), getMappedPort(SCHEMA_REGISTRY_INTERNAL_PORT));
        }
    }
}
