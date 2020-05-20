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
import com.hazelcast.sql.SqlCursor;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static java.util.Arrays.asList;

public class SqlKafkaTest extends SqlTestSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;

    private static KafkaTestSupport kafkaTestSupport;

    private String topicName;

    @BeforeClass
    public static void beforeClass() throws IOException {
        kafkaTestSupport = new KafkaTestSupport();
        kafkaTestSupport.createKafkaCluster();
    }

    @Before
    public void before() {
        topicName = "t_" + randomString().replace('-', '_');
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);

        sqlService.query("CREATE EXTERNAL TABLE " + topicName + " (__key INT, this VARCHAR) " +
                "TYPE \"com.hazelcast.Kafka\" " + // TODO extract to constant
                "OPTIONS (" +
                "  \"bootstrap.servers\" '" + kafkaTestSupport.getBrokerConnectionString() + "', " +
                "  \"key.serializer\" '" + IntegerSerializer.class.getCanonicalName() + "', " +
                "  \"key.deserializer\" '" + IntegerDeserializer.class.getCanonicalName() + "', " +
                "  \"value.serializer\" '" + StringSerializer.class.getCanonicalName() + "', " +
                "  \"value.deserializer\" '" + StringDeserializer.class.getCanonicalName() + "', " +
                "  \"auto.offset.reset\" 'earliest'" +
                ")");
    }

    @AfterClass
    public static void afterClass() {
        if (kafkaTestSupport != null) {
            kafkaTestSupport.shutdownKafkaCluster();
            kafkaTestSupport = null;
        }
    }

    @Test
    public void stream_kafka() {
        kafkaTestSupport.produce(topicName, 0, "value-" + 0);
        kafkaTestSupport.produce(topicName, 1, "value-" + 1);
        kafkaTestSupport.produce(topicName, 2, "value-" + 2);

        assertRowsEventuallyAnyOrder(
                "SELECT * FROM " + topicName,
                asList(
                        new Row(0, "value-0"),
                        new Row(1, "value-1"),
                        new Row(2, "value-2")));
    }

    @Test
    public void write_kafka() {
        assertTopic(topicName, "INSERT INTO " + topicName + " VALUES(1, 'value-1')",
                createMap(1, "value-1"));
    }

    @Test
    public void write_kafkaWithOverwrite() {
        assertTopic(topicName, "INSERT OVERWRITE " + topicName + " VALUES(1, 'value-1')",
                createMap(1, "value-1"));
    }

    private void assertTopic(String name, String sql, Map<Integer, String> expected) {
        SqlCursor cursor = sqlService.query(sql);

        cursor.iterator().forEachRemaining(o -> { });

        kafkaTestSupport.assertTopicContentsEventually(name, expected, false);
    }
}
