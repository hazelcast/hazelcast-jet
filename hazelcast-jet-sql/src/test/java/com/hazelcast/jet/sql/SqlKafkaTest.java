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

import com.hazelcast.jet.Job;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.schema.JetSchema.KAFKA_CONNECTOR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SqlKafkaTest extends SimpleTestInClusterSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;

    private static JetSqlService sqlService;
    private static KafkaTestSupport kafkaTestSupport;

    private String topicName;

    @BeforeClass
    public static void beforeClass() throws IOException {
        kafkaTestSupport = new KafkaTestSupport();
        kafkaTestSupport.createKafkaCluster();
        initialize(1, null);

        sqlService = new JetSqlService(instance());

        sqlService.createServer("kafka_test_server", KAFKA_CONNECTOR, createMap(
                "bootstrap.servers", kafkaTestSupport.getBrokerConnectionString(),
                "key.deserializer", IntegerDeserializer.class.getCanonicalName(),
                "value.deserializer", StringDeserializer.class.getCanonicalName(),
                "auto.offset.reset", "earliest"));
    }

    @Before
    public void before() {
        topicName = "t_" + randomString().replace('-', '_');
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);

        List<Entry<String, QueryDataType>> intToStringMapFields = asList(
                entry("__key", QueryDataType.INT),
                entry("this", QueryDataType.VARCHAR));

        sqlService.createTable(topicName, "kafka_test_server",
                emptyMap(),
                intToStringMapFields);
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
    public void write_kafka() throws Exception {
        fail("todo");
    }

    private <K, V> void assertMap(String mapName, String sql, Map<K, V> expected) {
        Job job = sqlService.execute(sql);
        job.join();
        assertEquals(expected, new HashMap<>(instance().getMap(mapName)));
    }

    private void assertRowsEventuallyAnyOrder(String sql, Collection<Row> expectedRows) {
        Observable<Object[]> observable = sqlService.executeQuery(sql);
        BlockingQueue<Object[]> rows = new LinkedBlockingQueue<>();
        observable.addObserver(row -> rows.add(row));

        assertTrueEventually(() -> assertEquals(expectedRows.size(), rows.size()));

        assertEquals(new HashSet<>(expectedRows), rows.stream().map(Row::new).collect(toSet()));
    }

    private static final class Row {
        Object[] values;

        Row(Object ... values) {
            this.values = values;
        }

        @Override
        public String toString() {
            return "Row{" + Arrays.toString(values) + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Row row = (Row) o;
            return Arrays.equals(values, row.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }
}
