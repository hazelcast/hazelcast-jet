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
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

// using Kafka as we want to test Jet optimization rules (with just IMap(s) in the query it would not be
// the case, query would be executed by IMDG), we might want to switch to something lighter/faster when available
public class SqlTest extends JetSqlTestSupport {

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
    public static void afterClass() {
        if (kafkaTestSupport != null) {
            kafkaTestSupport.shutdownKafkaCluster();
            kafkaTestSupport = null;
        }
    }

    @Test
    public void test_values() {
        assertEmpty(
                "SELECT a - b FROM (VALUES (1, 2)) AS t (a, b) WHERE a + b > 4"
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT a - b FROM (VALUES (7, 11)) AS t (a, b) WHERE a + b > 4",
                singletonList(new Row((byte) -4))
        );
    }

    @Test
    public void test_unicodeConstant() {
        String name = createRandomTopic();
        kafkaTestSupport.produce(name, 0, "value-" + 0);
        kafkaTestSupport.produce(name, 1, "value-" + 1);

        assertRowsEventuallyInAnyOrder(
                "SELECT '喷气式飞机' FROM " + name,
                asList(
                        new Row("喷气式飞机"),
                        new Row("喷气式飞机")));
    }

    @Test
    public void test_fullScan() {
        String name = createRandomTopic();
        kafkaTestSupport.produce(name, 0, "value-" + 0);
        kafkaTestSupport.produce(name, 1, "value-" + 1);

        assertRowsEventuallyInAnyOrder(
                "SELECT this, __key FROM " + name,
                asList(
                        new Row("value-0", 0),
                        new Row("value-1", 1)));
    }

    @Test
    public void test_fullScanStar() {
        String name = createRandomTopic();
        kafkaTestSupport.produce(name, 0, "value-" + 0);
        kafkaTestSupport.produce(name, 1, "value-" + 1);

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row(0, "value-0"),
                        new Row(1, "value-1")));
    }

    @Test
    public void test_fullScanFilter() {
        String name = createRandomTopic();
        kafkaTestSupport.produce(name, 0, "value-" + 0);
        kafkaTestSupport.produce(name, 1, "value-" + 1);
        kafkaTestSupport.produce(name, 2, "value-" + 2);

        assertRowsEventuallyInAnyOrder(
                "SELECT this FROM " + name + " WHERE __key=1 or this='value-0'",
                asList(
                        new Row("value-0"),
                        new Row("value-1")));
    }

    @Test
    public void test_fullScanProjection1() {
        String name = createRandomTopic();
        kafkaTestSupport.produce(name, 0, "value-" + 0);
        kafkaTestSupport.produce(name, 1, "value-" + 1);

        assertRowsEventuallyInAnyOrder(
                "SELECT UPPER(this) FROM " + name + " WHERE this='value-1'",
                singletonList(new Row("VALUE-1")));
    }

    @Test
    public void test_fullScanProjection2() {
        String name = createRandomTopic();
        kafkaTestSupport.produce(name, 0, "value-" + 0);
        kafkaTestSupport.produce(name, 1, "value-" + 1);

        assertRowsEventuallyInAnyOrder(
                "SELECT this FROM " + name + " WHERE UPPER(this)='VALUE-1'",
                singletonList(new Row("value-1")));
    }

    @Test
    public void fullScan_projection3() {
        String name = createRandomTopic();
        kafkaTestSupport.produce(name, 0, "value-" + 0);
        kafkaTestSupport.produce(name, 1, "value-" + 1);

        assertRowsEventuallyInAnyOrder(
                "SELECT this FROM (SELECT UPPER(this) this FROM " + name + ") WHERE this='VALUE-1'",
                singletonList(new Row("VALUE-1")));
    }

    @Test
    public void test_fullScanProjection4() {
        String name = createRandomTopic();
        kafkaTestSupport.produce(name, 0, "value-" + 0);
        kafkaTestSupport.produce(name, 1, "value-" + 1);

        assertRowsEventuallyInAnyOrder(
                "SELECT UPPER(this) FROM " + name + " WHERE UPPER(this)='VALUE-1'",
                singletonList(new Row("VALUE-1")));
    }

    private static String createRandomTopic() {
        String topicName = "t_" + randomString().replace('-', '_');
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);

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

        return topicName;
    }
}
