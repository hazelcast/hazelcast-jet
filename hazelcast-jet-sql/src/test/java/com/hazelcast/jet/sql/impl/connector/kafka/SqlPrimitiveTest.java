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
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlService;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_OBJECT_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static java.util.Arrays.asList;

public class SqlPrimitiveTest extends JetSqlTestSupport {

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
    public void test_insertSelect() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " "
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + " "
                + "OPTIONS ( "
                + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", " + OPTION_KEY_CLASS + " '" + Integer.class.getName() + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_VALUE_CLASS + "\" '" + String.class.getName() + "'"
                + ", bootstrap.servers '" + kafkaTestSupport.getBrokerConnectionString() + "'"
                + ", key.serializer '" + IntegerSerializer.class.getCanonicalName() + "'"
                + ", key.deserializer '" + IntegerDeserializer.class.getCanonicalName() + "'"
                + ", \"value.serializer\" '" + StringSerializer.class.getCanonicalName() + "'"
                + ", \"value.deserializer\" '" + StringDeserializer.class.getCanonicalName() + "'"
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        IMap<Integer, String> source = instance().getMap("source");
        source.put(0, "value-0");
        source.put(1, "value-1");

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " SELECT * FROM " + source.getName(),
                createMap(0, "value-0", 1, "value-1")
        );
    }

    @Test
    public void test_insertValues() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " "
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + " "
                + "OPTIONS ( "
                + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", " + OPTION_KEY_CLASS + " '" + Integer.class.getName() + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_VALUE_CLASS + "\" '" + String.class.getName() + "'"
                + ", bootstrap.servers '" + kafkaTestSupport.getBrokerConnectionString() + "'"
                + ", key.serializer '" + IntegerSerializer.class.getCanonicalName() + "'"
                + ", key.deserializer '" + IntegerDeserializer.class.getCanonicalName() + "'"
                + ", \"value.serializer\" '" + StringSerializer.class.getCanonicalName() + "'"
                + ", \"value.deserializer\" '" + StringDeserializer.class.getCanonicalName() + "'"
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (__key, this) VALUES (1, '2')",
                createMap(1, "2")
        );
    }

    @Test
    public void test_insertOverwrite() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " "
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + " "
                + "OPTIONS ( "
                + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", " + OPTION_KEY_CLASS + " '" + Integer.class.getName() + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_VALUE_CLASS + "\" '" + String.class.getName() + "'"
                + ", bootstrap.servers '" + kafkaTestSupport.getBrokerConnectionString() + "'"
                + ", key.serializer '" + IntegerSerializer.class.getCanonicalName() + "'"
                + ", key.deserializer '" + IntegerDeserializer.class.getCanonicalName() + "'"
                + ", \"value.serializer\" '" + StringSerializer.class.getCanonicalName() + "'"
                + ", \"value.deserializer\" '" + StringDeserializer.class.getCanonicalName() + "'"
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT OVERWRITE " + name + " (this, __key) VALUES ('2', 1)",
                createMap(1, "2")
        );
    }

    @Test
    public void test_insertWithProject() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " "
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + " "
                + "OPTIONS ( "
                + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", " + OPTION_KEY_CLASS + " '" + Integer.class.getName() + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_VALUE_CLASS + "\" '" + String.class.getName() + "'"
                + ", bootstrap.servers '" + kafkaTestSupport.getBrokerConnectionString() + "'"
                + ", key.serializer '" + IntegerSerializer.class.getCanonicalName() + "'"
                + ", key.deserializer '" + IntegerDeserializer.class.getCanonicalName() + "'"
                + ", \"value.serializer\" '" + StringSerializer.class.getCanonicalName() + "'"
                + ", \"value.deserializer\" '" + StringDeserializer.class.getCanonicalName() + "'"
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (this, __key) VALUES (2, CAST(0 + 1 AS INT))",
                createMap(1, "2")
        );
    }

    @Test
    public void test_fieldsMapping() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME __key"
                + ", name VARCHAR EXTERNAL NAME this"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + " "
                + "OPTIONS ( "
                + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", " + OPTION_KEY_CLASS + " '" + Integer.class.getName() + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_VALUE_CLASS + "\" '" + String.class.getName() + "'"
                + ", bootstrap.servers '" + kafkaTestSupport.getBrokerConnectionString() + "'"
                + ", key.serializer '" + IntegerSerializer.class.getCanonicalName() + "'"
                + ", key.deserializer '" + IntegerDeserializer.class.getCanonicalName() + "'"
                + ", \"value.serializer\" '" + StringSerializer.class.getCanonicalName() + "'"
                + ", \"value.deserializer\" '" + StringDeserializer.class.getCanonicalName() + "'"
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (id, name) VALUES (2, 'value-2')",
                createMap(2, "value-2")
        );
    }

    @Test
    public void test_topicNameAndTableNameDifferent() {
        String topicName = createRandomTopic();
        String tableName = generateRandomName();

        sqlService.execute("CREATE MAPPING " + tableName + " "
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + " "
                + "OPTIONS ( "
                + OPTION_OBJECT_NAME + " '" + topicName + "'"
                + ", " + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", " + OPTION_KEY_CLASS + " '" + Integer.class.getName() + "'"
                + ", \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT + "'"
                + ", \"" + OPTION_VALUE_CLASS + "\" '" + String.class.getName() + "'"
                + ", bootstrap.servers '" + kafkaTestSupport.getBrokerConnectionString() + "'"
                + ", key.serializer '" + IntegerSerializer.class.getCanonicalName() + "'"
                + ", key.deserializer '" + IntegerDeserializer.class.getCanonicalName() + "'"
                + ", \"value.serializer\" '" + StringSerializer.class.getCanonicalName() + "'"
                + ", \"value.deserializer\" '" + StringDeserializer.class.getCanonicalName() + "'"
                + ", \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        kafkaTestSupport.produce(topicName, 1, "Alice");
        kafkaTestSupport.produce(topicName, 2, "Bob");

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + tableName,
                asList(
                        new Row(1, "Alice"),
                        new Row(2, "Bob")
                )
        );
    }

    private static String generateRandomName() {
        return "primitive_" + randomString().replace('-', '_');
    }

    private static String createRandomTopic() {
        String topicName = "t_" + randomString().replace('-', '_');
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        return topicName;
    }

    private static void assertTopicEventually(String name, String sql, Map<Integer, String> expected) {
        sqlService.execute(sql);

        kafkaTestSupport.assertTopicContentsEventually(name, expected, false);
    }
}
