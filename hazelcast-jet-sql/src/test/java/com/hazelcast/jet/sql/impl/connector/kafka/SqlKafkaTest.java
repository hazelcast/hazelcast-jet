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
import com.hazelcast.jet.sql.impl.connector.kafka.model.BigIntegerDeserializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.BigIntegerSerializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.Person;
import com.hazelcast.jet.sql.impl.connector.kafka.model.PersonDeserializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.PersonSerializer;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class SqlKafkaTest extends JetSqlTestSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;

    private static KafkaTestSupport kafkaTestSupport;

    private static SqlService sqlService;

    private String topicName;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @BeforeClass
    public static void beforeClass() throws IOException {
        kafkaTestSupport = new KafkaTestSupport();
        kafkaTestSupport.createKafkaCluster();
    }

    @Before
    public void before() {
        topicName = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + topicName + " "
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + " "
                + "OPTIONS ( "
                + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JAVA_SERIALIZATION_FORMAT
                + "', " + OPTION_KEY_CLASS + " '" + Integer.class.getName()
                + "', \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT
                + "', \"" + OPTION_VALUE_CLASS + "\" '" + String.class.getName()
                + "', bootstrap.servers '" + kafkaTestSupport.getBrokerConnectionString()
                + "', key.serializer '" + IntegerSerializer.class.getCanonicalName()
                + "', key.deserializer '" + IntegerDeserializer.class.getCanonicalName()
                + "', \"value.serializer\" '" + StringSerializer.class.getCanonicalName()
                + "', \"value.deserializer\" '" + StringDeserializer.class.getCanonicalName()
                + "', \"auto.offset.reset\" 'earliest'"
                + ")"
        );
    }

    @AfterClass
    public static void afterClass() {
        if (kafkaTestSupport != null) {
            kafkaTestSupport.shutdownKafkaCluster();
            kafkaTestSupport = null;
        }
    }

    @Test
    public void insert() {
        assertTopic(topicName, "INSERT INTO " + topicName + " VALUES (1, 'value-1')",
                createMap(1, "value-1"));
    }

    @Test
    public void insert_overwrite() {
        assertTopic(topicName, "INSERT OVERWRITE " + topicName + " (this, __key) VALUES ('value-1', 1)",
                createMap(1, "value-1"));
    }

    @Test
    public void select_convert() {
        String topicName = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + topicName + " "
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + " "
                + "OPTIONS ( "
                + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JAVA_SERIALIZATION_FORMAT
                + "', " + OPTION_KEY_CLASS + " '" + BigInteger.class.getName()
                + "', \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT
                + "', \"" + OPTION_VALUE_CLASS + "\" '" + String.class.getName()
                + "', bootstrap.servers '" + kafkaTestSupport.getBrokerConnectionString()
                + "', key.serializer '" + BigIntegerSerializer.class.getCanonicalName()
                + "', key.deserializer '" + BigIntegerDeserializer.class.getCanonicalName()
                + "', \"value.serializer\" '" + StringSerializer.class.getCanonicalName()
                + "', \"value.deserializer\" '" + StringDeserializer.class.getCanonicalName()
                + "', \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        sqlService.execute("INSERT INTO " + topicName + " VALUES (12, 'a')");

        assertRowsEventuallyInAnyOrder(
                "SELECT __key + 1, this FROM " + topicName,
                singletonList(new Row(BigDecimal.valueOf(13), "a")));
    }

    @Test
    public void select_pojo() {
        String topicName = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + topicName + " "
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + " "
                + "OPTIONS ( "
                + OPTION_SERIALIZATION_KEY_FORMAT + " '" + JAVA_SERIALIZATION_FORMAT
                + "', " + OPTION_KEY_CLASS + " '" + Integer.class.getName()
                + "', \"" + OPTION_SERIALIZATION_VALUE_FORMAT + "\" '" + JAVA_SERIALIZATION_FORMAT
                + "', \"" + OPTION_VALUE_CLASS + "\" '" + Person.class.getName()
                + "', bootstrap.servers '" + kafkaTestSupport.getBrokerConnectionString()
                + "', key.serializer '" + IntegerSerializer.class.getCanonicalName()
                + "', key.deserializer '" + IntegerDeserializer.class.getCanonicalName()
                + "', \"value.serializer\" '" + PersonSerializer.class.getCanonicalName()
                + "', \"value.deserializer\" '" + PersonDeserializer.class.getCanonicalName()
                + "', \"auto.offset.reset\" 'earliest'"
                + ")"
        );

        sqlService.execute("INSERT INTO " + topicName + " (__key, name, age) VALUES (0, 'Alice', 30)");
        sqlService.execute("INSERT INTO " + topicName + " (__key, name, age) VALUES (1, 'Bob', 40)");

        assertRowsEventuallyInAnyOrder(
                "SELECT __key, name, age FROM " + topicName,
                asList(
                        new Row(0, "Alice", 30),
                        new Row(1, "Bob", 40)));
    }

    @Test
    public void select_unicodeConstant() {
        kafkaTestSupport.produce(topicName, 0, "value-" + 0);
        kafkaTestSupport.produce(topicName, 1, "value-" + 1);

        assertRowsEventuallyInAnyOrder(
                "SELECT '喷气式飞机' FROM " + topicName,
                asList(
                        new Row("喷气式飞机"),
                        new Row("喷气式飞机")));
    }

    @Test
    public void fullScan() {
        kafkaTestSupport.produce(topicName, 0, "value-" + 0);
        kafkaTestSupport.produce(topicName, 1, "value-" + 1);

        assertRowsEventuallyInAnyOrder(
                "SELECT this, __key FROM " + topicName,
                asList(
                        new Row("value-0", 0),
                        new Row("value-1", 1)));
    }

    @Test
    public void fullScan_star() {
        kafkaTestSupport.produce(topicName, 0, "value-" + 0);
        kafkaTestSupport.produce(topicName, 1, "value-" + 1);

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + topicName,
                asList(
                        new Row(0, "value-0"),
                        new Row(1, "value-1")));
    }

    @Test
    public void fullScan_filter() {
        kafkaTestSupport.produce(topicName, 0, "value-" + 0);
        kafkaTestSupport.produce(topicName, 1, "value-" + 1);
        kafkaTestSupport.produce(topicName, 2, "value-" + 2);

        assertRowsEventuallyInAnyOrder(
                "SELECT this FROM " + topicName + " WHERE __key=1 or this='value-0'",
                asList(
                        new Row("value-0"),
                        new Row("value-1")));
    }

    @Test
    public void fullScan_projection1() {
        kafkaTestSupport.produce(topicName, 0, "value-" + 0);
        kafkaTestSupport.produce(topicName, 1, "value-" + 1);

        assertRowsEventuallyInAnyOrder(
                "SELECT upper(this) FROM " + topicName + " WHERE this='value-1'",
                singletonList(new Row("VALUE-1")));
    }

    @Test
    public void fullScan_projection2() {
        kafkaTestSupport.produce(topicName, 0, "value-" + 0);
        kafkaTestSupport.produce(topicName, 1, "value-" + 1);

        assertRowsEventuallyInAnyOrder(
                "SELECT this FROM " + topicName + " WHERE upper(this)='VALUE-1'",
                singletonList(new Row("value-1")));
    }

    @Test
    public void fullScan_projection3() {
        kafkaTestSupport.produce(topicName, 0, "value-" + 0);
        kafkaTestSupport.produce(topicName, 1, "value-" + 1);

        assertRowsEventuallyInAnyOrder(
                "SELECT this FROM (SELECT upper(this) this FROM " + topicName + ") WHERE this='VALUE-1'",
                singletonList(new Row("VALUE-1")));
    }

    @Test
    public void fullScan_projection4() {
        kafkaTestSupport.produce(topicName, 0, "value-" + 0);
        kafkaTestSupport.produce(topicName, 1, "value-" + 1);

        assertRowsEventuallyInAnyOrder(
                "SELECT upper(this) FROM " + topicName + " WHERE upper(this)='VALUE-1'",
                singletonList(new Row("VALUE-1")));
    }

    private static String createRandomTopic() {
        String topicName = "t_" + randomString().replace('-', '_');
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        return topicName;
    }

    private static void assertTopic(String name, String sql, Map<Integer, String> expected) {
        sqlService.execute(sql);

        kafkaTestSupport.assertTopicContentsEventually(name, expected, false);
    }
}
