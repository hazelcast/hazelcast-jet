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
import com.hazelcast.sql.SqlService;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_VALUE_FORMAT;
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
                + ", key.serializer '" + StringSerializer.class.getCanonicalName() + "'"
                + ", key.deserializer '" + StringDeserializer.class.getCanonicalName() + "'"
                + ", \"value.serializer\" '" + StringSerializer.class.getCanonicalName() + "'"
                + ", \"value.deserializer\" '" + StringDeserializer.class.getCanonicalName() + "'"
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
                + ", key.serializer '" + StringSerializer.class.getCanonicalName() + "'"
                + ", key.deserializer '" + StringDeserializer.class.getCanonicalName() + "'"
                + ", \"value.serializer\" '" + StringSerializer.class.getCanonicalName() + "'"
                + ", \"value.deserializer\" '" + StringDeserializer.class.getCanonicalName() + "'"
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
                + ", key.serializer '" + StringSerializer.class.getCanonicalName() + "'"
                + ", key.deserializer '" + StringDeserializer.class.getCanonicalName() + "'"
                + ", \"value.serializer\" '" + StringSerializer.class.getCanonicalName() + "'"
                + ", \"value.deserializer\" '" + StringDeserializer.class.getCanonicalName() + "'"
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
                + ", key.serializer '" + StringSerializer.class.getCanonicalName() + "'"
                + ", key.deserializer '" + StringDeserializer.class.getCanonicalName() + "'"
                + ", \"value.serializer\" '" + StringSerializer.class.getCanonicalName() + "'"
                + ", \"value.deserializer\" '" + StringDeserializer.class.getCanonicalName() + "'"
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
