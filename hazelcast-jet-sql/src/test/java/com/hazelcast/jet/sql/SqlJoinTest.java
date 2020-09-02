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

import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import com.hazelcast.sql.SqlService;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static com.hazelcast.jet.sql.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_VALUE_CLASS;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlJoinTest extends JetSqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getHazelcastInstance().getSql();
    }

    private static final int INITIAL_PARTITION_COUNT = 4;

    private static KafkaTestSupport kafkaTestSupport;

    private String topicName;
    private String mapName;

    @BeforeClass
    public static void beforeClass() throws IOException {
        kafkaTestSupport = new KafkaTestSupport();
        kafkaTestSupport.createKafkaCluster();
    }

    @Before
    public void before() {
        topicName = "k_" + randomString().replace('-', '_');
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        sqlService.execute(format("CREATE EXTERNAL TABLE %s " +
                        "TYPE \"%s\" " +
                        "OPTIONS (" +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"%s\" '%s'," +
                        " \"bootstrap.servers\" '%s'," +
                        " \"key.serializer\" '%s'," +
                        " \"key.deserializer\" '%s'," +
                        " \"value.serializer\" '%s'," +
                        " \"value.deserializer\" '%s'," +
                        " \"auto.offset.reset\" 'earliest'" +
                        ")",
                topicName, KafkaSqlConnector.TYPE_NAME,
                OPTION_SERIALIZATION_KEY_FORMAT, JAVA_SERIALIZATION_FORMAT,
                OPTION_KEY_CLASS, Integer.class.getName(),
                OPTION_SERIALIZATION_VALUE_FORMAT, JAVA_SERIALIZATION_FORMAT,
                OPTION_VALUE_CLASS, String.class.getName(),
                kafkaTestSupport.getBrokerConnectionString(),
                IntegerSerializer.class.getCanonicalName(), IntegerDeserializer.class.getCanonicalName(),
                StringSerializer.class.getCanonicalName(), StringDeserializer.class.getCanonicalName()
        ));

        mapName = createMapWithRandomName();
    }

    @AfterClass
    public static void afterClass() {
        if (kafkaTestSupport != null) {
            kafkaTestSupport.shutdownKafkaCluster();
            kafkaTestSupport = null;
        }
    }

    @Test
    public void enrichment_join() {
        kafkaTestSupport.produce(topicName, 0, "kafka-value-0");
        kafkaTestSupport.produce(topicName, 1, "kafka-value-1");
        kafkaTestSupport.produce(topicName, 2, "kafka-value-2");

        instance().getMap(mapName).put(1, "map-value-1");
        instance().getMap(mapName).put(2, "map-value-2");
        instance().getMap(mapName).put(3, "map-value-3");

        assertRowsEventuallyAnyOrder(
                format("SELECT k.__key, k.this, m.__key, m.this " +
                                "FROM %s k " +
                                "JOIN %s m ON k.__key = m.__key",
                        topicName, mapName
                ),
                asList(
                        new Row(1, "kafka-value-1", 1, "map-value-1"),
                        new Row(2, "kafka-value-2", 2, "map-value-2")
                ));
    }

    @Test
    public void enrichment_join_with_filter() {
        kafkaTestSupport.produce(topicName, 0, "kafka-value-0");
        kafkaTestSupport.produce(topicName, 1, "kafka-value-1");
        kafkaTestSupport.produce(topicName, 2, "kafka-value-2");
        kafkaTestSupport.produce(topicName, 3, "kafka-value-3");

        instance().getMap(mapName).put(1, "map-value-1");
        instance().getMap(mapName).put(2, "map-value-2");
        instance().getMap(mapName).put(3, "map-value-3");

        assertRowsEventuallyAnyOrder(
                format("SELECT k.__key, m.this " +
                                "FROM %s k " +
                                "JOIN %s m ON k.__key = m.__key " +
                                "WHERE k.__key > 1 AND m.__key < 3",
                        topicName, mapName
                ),
                singletonList(
                        new Row(2, "map-value-2")
                ));
    }

    @Test
    public void enrichment_join_with_project() {
        kafkaTestSupport.produce(topicName, 1, "kafka-value-1");
        kafkaTestSupport.produce(topicName, 2, "kafka-value-2");
        kafkaTestSupport.produce(topicName, 3, "kafka-value-3");

        instance().getMap(mapName).put(0, "map-value-0");
        instance().getMap(mapName).put(1, "map-value-1");
        instance().getMap(mapName).put(2, "map-value-2");

        assertRowsEventuallyAnyOrder(
                format("SELECT k.__key + m.__key, UPPER(k.this) " +
                                "FROM %s k " +
                                "JOIN %s m ON k.__key = m.__key",
                        topicName, mapName
                ),
                asList(
                        new Row(2L, "KAFKA-VALUE-1"),
                        new Row(4L, "KAFKA-VALUE-2")
                ));
    }

    @Test
    public void enrichment_join_with_value_condition() {
        kafkaTestSupport.produce(topicName, 0, "value-0");
        kafkaTestSupport.produce(topicName, 1, "value-1");
        kafkaTestSupport.produce(topicName, 2, "value-2");

        instance().getMap(mapName).put(3, "value-1");
        instance().getMap(mapName).put(4, "value-2");
        instance().getMap(mapName).put(5, "value-3");

        assertRowsEventuallyAnyOrder(
                format("SELECT k.__key, k.this, m.__key, m.this " +
                                "FROM %s k " +
                                "JOIN %s m ON k.this = m.this",
                        topicName, mapName
                ),
                asList(
                        new Row(1, "value-1", 3, "value-1"),
                        new Row(2, "value-2", 4, "value-2")
                ));
    }

    @Test
    public void enrichment_join_with_non_equi_condition() {
        kafkaTestSupport.produce(topicName, 1, "kafka-value-1");
        kafkaTestSupport.produce(topicName, 2, "kafka-value-2");
        kafkaTestSupport.produce(topicName, 3, "kafka-value-3");

        instance().getMap(mapName).put(0, "map-value-0");
        instance().getMap(mapName).put(1, "map-value-1");
        instance().getMap(mapName).put(2, "map-value-2");

        assertRowsEventuallyAnyOrder(
                format("SELECT k.__key, m.__key " +
                                "FROM %s k " +
                                "JOIN %s m ON k.__key > m.__key",
                        topicName, mapName
                ),
                asList(
                        new Row(1, 0),
                        new Row(2, 0),
                        new Row(2, 1),
                        new Row(3, 0),
                        new Row(3, 1),
                        new Row(3, 2)
                ));
    }

    @Test
    public void enrichment_join_with_multiple_tables() {
        kafkaTestSupport.produce(topicName, 1, "kafka-value-1");
        kafkaTestSupport.produce(topicName, 2, "kafka-value-2");
        kafkaTestSupport.produce(topicName, 3, "kafka-value-3");

        instance().getMap(mapName).put(0, "map1-value-0");
        instance().getMap(mapName).put(1, "map1-value-1");
        instance().getMap(mapName).put(2, "map1-value-2");

        String anotherMapName = createMapWithRandomName();
        instance().getMap(anotherMapName).put(0, "map2-value-0");
        instance().getMap(anotherMapName).put(1, "map2-value-1");
        instance().getMap(anotherMapName).put(2, "map2-value-2");

        assertRowsEventuallyAnyOrder(
                format("SELECT k.__key, m1.this, m2.this " +
                                "FROM %s k " +
                                "JOIN %s m1 ON k.__key = m1.__key " +
                                "JOIN %s m2 ON k.__key + m1.__key > m2.__key",
                        topicName, mapName, anotherMapName
                ),
                asList(
                        new Row(1, "map1-value-1", "map2-value-0"),
                        new Row(1, "map1-value-1", "map2-value-1"),
                        new Row(2, "map1-value-2", "map2-value-0"),
                        new Row(2, "map1-value-2", "map2-value-1"),
                        new Row(2, "map1-value-2", "map2-value-2")
                ));
    }

    @Test
    public void enrichment_join_fails_for_not_supported_connector() {
        assertThatThrownBy(() -> sqlService.execute(
                format("SELECT 1 FROM %s k JOIN %s m ON m.__key = k.__key", mapName, topicName)
        )).hasCauseInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("Nested loop reader not supported for " + KafkaSqlConnector.class.getName());
    }

    private static String createMapWithRandomName() {
        String mapName = "m_" + randomString().replace('-', '_');
        sqlService.execute(javaSerializableMapDdl(mapName, Integer.class, String.class));
        return mapName;
    }
}
