/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cdc.impl;

import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.cdc.ChangeEvent;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.SourceBuilder;
import io.debezium.transforms.ExtractNewRecordState;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

class CdcSource {

    private final SourceConnector connector;
    private final SourceTask task;
    private final Map<String, String> taskConfig;
    private final ExtractNewRecordState<SourceRecord> smt;

    /**
     * Key represents the partition which the record originated from. Value
     * represents the offset within that partition. Kafka Connect represents
     * the partition and offset as arbitrary values so that is why it is
     * stored as map.
     * See {@link SourceRecord} for more information regarding the format.
     */
    private Map<Map<String, ?>, Map<String, ?>> partitionsToOffset = new HashMap<>();
    private boolean taskInit;

    CdcSource(Processor.Context ctx, Properties properties) {
        try {
            String connectorClazz = properties.getProperty("connector.class");
            Class<?> connectorClass = Thread.currentThread().getContextClassLoader().loadClass(connectorClazz);
            connector = (SourceConnector) connectorClass.getConstructor().newInstance();
            connector.initialize(new JetConnectorContext());
            connector.start((Map) injectHazelcastInstanceNameProperty(ctx, properties));

            smt = initSmt();

            taskConfig = connector.taskConfigs(1).get(0);
            task = (SourceTask) connector.taskClass().getConstructor().newInstance();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    public void fillBuffer(SourceBuilder.TimestampedSourceBuffer<ChangeEvent> buf) {
        if (!taskInit) {
            task.initialize(new JetSourceTaskContext());
            task.start(taskConfig);
            taskInit = true;
        }
        try {
            List<SourceRecord> records = task.poll();
            if (records == null) {
                return;
            }

            for (SourceRecord record : records) {
                boolean added = addToBuffer(record, buf);
                if (added) {
                    partitionsToOffset.put(record.sourcePartition(), record.sourceOffset());
                }
            }
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    private boolean addToBuffer(SourceRecord record, SourceBuilder.TimestampedSourceBuffer<ChangeEvent> buf) {
        record = smt.apply(record);
        if (record != null) {
            ChangeEvent event = extractEvent(record);
            long timestamp = extractTimestamp(record);
            buf.add(event, timestamp);
            return true;
        }
        return false;
    }

    public void destroy() {
        try {
            task.stop();
        } finally {
            connector.stop();
        }
    }

    public Map<Map<String, ?>, Map<String, ?>> createSnapshot() {
        return partitionsToOffset;
    }

    public void restoreSnapshot(List<Map<Map<String, ?>, Map<String, ?>>> snapshots) {
        this.partitionsToOffset = snapshots.get(0);
    }

    private static ExtractNewRecordState<SourceRecord> initSmt() {
        ExtractNewRecordState<SourceRecord> smt = new ExtractNewRecordState<>();

        Map<String, String> config = new HashMap<>();
        config.put("add.fields", "op, ts_ms");
        config.put("delete.handling.mode", "rewrite");
        smt.configure(config);

        return smt;
    }

    private static ChangeEvent extractEvent(SourceRecord record) {
        String keyJson = Values.convertToString(record.keySchema(), record.key());
        String valueJson = Values.convertToString(record.valueSchema(), record.value());
        return new ChangeEventJsonImpl(keyJson, valueJson);
    }

    private static long extractTimestamp(SourceRecord record) {
        if (record.valueSchema().field("__ts_ms") == null) {
            return 0L;
        } else {
            return ((Struct) record.value()).getInt64("__ts_ms");
        }
    }

    private class SourceOffsetStorageReader implements OffsetStorageReader {
        @Override
        public <V> Map<String, Object> offset(Map<String, V> partition) {
            return offsets(Collections.singletonList(partition)).get(partition);
        }

        @Override
        public <V> Map<Map<String, V>, Map<String, Object>> offsets(Collection<Map<String, V>> partitions) {
            Map<Map<String, V>, Map<String, Object>> map = new HashMap<>();
            for (Map<String, V> partition : partitions) {
                Map<String, Object> offset = (Map<String, Object>) partitionsToOffset.get(partition);
                map.put(partition, offset);
            }
            return map;
        }
    }

    private static Properties injectHazelcastInstanceNameProperty(Processor.Context ctx, Properties properties) {
        JetInstance jet = ctx.jetInstance();
        String instanceName = HazelcastInstanceFactory.getInstanceName(jet.getName(),
                jet.getHazelcastInstance().getConfig());
        properties.setProperty("database.history.hazelcast.instance.name", instanceName);
        //needed only by CDC sources... could be achieved via one more lambda, but we do have enough of those...
        return properties;
    }

    private static class JetConnectorContext implements ConnectorContext {
        @Override
        public void requestTaskReconfiguration() {
            // no-op since it is not supported
        }

        @Override
        public void raiseError(Exception e) {
            throw rethrow(e);
        }
    }

    private class JetSourceTaskContext implements SourceTaskContext {
        @Override
        public Map<String, String> configs() {
            return taskConfig;
        }

        @Override
        public OffsetStorageReader offsetStorageReader() {
            return new SourceOffsetStorageReader();
        }
    }
}
