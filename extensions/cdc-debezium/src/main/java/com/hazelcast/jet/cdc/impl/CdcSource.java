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

package com.hazelcast.jet.cdc.impl;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.logging.ILogger;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.HistoryRecord;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public abstract class CdcSource<T> {

    public static final String CONNECTOR_CLASS_PROPERTY = "connector.class";
    public static final String SEQUENCE_EXTRACTOR_CLASS_PROPERTY = "sequence.extractor.class";
    public static final String RECONNECT_INTERVAL_MS = "reconnect.interval.ms";
    public static final String RECONNECT_BEHAVIOUR_PROPERTY = "reconnect.behaviour";

    public static final long DEFAULT_RECONNECT_INTERVAL_MS = 5_000;
    public static final ReconnectBehaviour DEFAULT_RECONNECT_BEHAVIOUR = ReconnectBehaviour.FAIL;

    private static final ThreadLocal<List<byte[]>> THREAD_LOCAL_HISTORY = new ThreadLocal<>();

    private final ILogger logger;

    private final Map<String, String> properties;
    private final SourceConnector connector;
    private final Map<String, String> taskConfig;

    private final long reconnectIntervalMs;
    private final ReconnectBehaviour reconnectBehaviour;

    private State state = new State();
    private SourceTask task;

    CdcSource(Processor.Context context, Properties properties) {
        this.logger = context.logger();
        this.properties = (Map<String, String>) (Map) properties;

        try {
            connector = newInstance(properties, CONNECTOR_CLASS_PROPERTY);
            connector.initialize(new JetConnectorContext());

            reconnectIntervalMs = getReconnectIntervalMs(properties);
            reconnectBehaviour = getReconnectBehaviour(properties);

            connect();
            taskConfig = connector.taskConfigs(1).get(0);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    public void destroy() {
        disconnect();
    }

    public void fillBuffer(SourceBuilder.TimestampedSourceBuffer<T> buf) {
        try {
            if (task == null) {
                task = startNewTask();
            }
        } catch (Exception e) {
            waitForInitRetry(reconnectBehaviour, e);
            return;
        }

        try {
            List<SourceRecord> records = task.poll();
            if (records == null) {
                return;
            }

            for (SourceRecord record : records) {
                boolean added = addToBuffer(record, buf);
                if (added) {
                    Map<String, ?> partition = record.sourcePartition();
                    Map<String, ?> offset = record.sourceOffset();
                    state.setOffset(partition, offset);
                }
            }
        } catch (ConnectException ce) {
            reconnect(reconnectBehaviour, ce);
        } catch (InterruptedException ie) {
            logger.warning("Waiting for data interrupted");
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private SourceTask startNewTask() throws Exception {
        SourceTask task = (SourceTask) connector.taskClass().getConstructor().newInstance();
        task.initialize(new JetSourceTaskContext());

        // Our DatabaseHistory implementation will be created by the
        // following start() call, on this thread (blocking worker
        // thread) and this is how we pass it the list it should
        // use for storing history records.
        THREAD_LOCAL_HISTORY.set(state.historyRecords);
        task.start(taskConfig);
        THREAD_LOCAL_HISTORY.remove();
        return task;
    }

    private boolean addToBuffer(SourceRecord sourceRecord, SourceBuilder.TimestampedSourceBuffer<T> buf) {
        T t = mapToOutput(sourceRecord);
        if (t != null) {
            long timestamp = extractTimestamp(sourceRecord);
            buf.add(t, timestamp);
            return true;
        }
        return false;
    }

    private void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            logger.warning("Sleep interruped!", e);
        }
    }

    private void reconnect(ReconnectBehaviour behaviour, ConnectException ce) {
        switch (behaviour) {
            case FAIL:
                throw new JetException("Database shutdown detected", ce);
            case CLEAR_STATE_AND_RECONNECT:
                logger.warning("Connection to database lost, will attempt to reconnect and retry operations from " +
                        "scratch: " + ce.getMessage());

                disconnect();
                state = new State();
                connect();
                return;
            case RECONNECT:
                logger.warning("Connection to database lost, will attempt to reconnect and resume operations: " +
                        ce.getMessage());

                disconnect();
                connect();
                return;
            default:
                throw new RuntimeException("Programming error, unhandled reconnect behaviour: " + behaviour);
        }
    }

    private void waitForInitRetry(ReconnectBehaviour behaviour, Exception e) {
        switch (behaviour) {
            case FAIL:
                logger.warning("Initializing connector task failed, giving up: " + e.getMessage());
                throw new JetException("Initializing connector task failed", e);
            case RECONNECT:
            case CLEAR_STATE_AND_RECONNECT:
                logger.warning("Initializing connector task failed, retrying in " + reconnectIntervalMs + "ms: " +
                        e.getMessage());
                sleepMs(reconnectIntervalMs);
                return;
            default:
                throw new RuntimeException("Programming error, unhandled reconnect behaviour: " + behaviour);
        }
    }

    private void connect() {
        connector.start(properties);
    }

    private void disconnect() {
        try {
            task.stop();
        } finally {
            task = null;
            connector.stop();
        }
    }

    public State createSnapshot() {
        return state;
    }

    public void restoreSnapshot(List<State> snapshots) {
        this.state = snapshots.get(0);
    }

    protected abstract T mapToOutput(SourceRecord record);

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

    private class SourceOffsetStorageReader implements OffsetStorageReader {
        @Override
        public <V> Map<String, Object> offset(Map<String, V> partition) {
            return offsets(Collections.singletonList(partition)).get(partition);
        }

        @Override
        public <V> Map<Map<String, V>, Map<String, Object>> offsets(Collection<Map<String, V>> partitions) {
            Map<Map<String, V>, Map<String, Object>> map = new HashMap<>();
            for (Map<String, V> partition : partitions) {
                Map<String, Object> offset = (Map<String, Object>) state.getOffset(partition);
                map.put(partition, offset);
            }
            return map;
        }
    }

    protected static <T> T newInstance(Properties properties, String classNameProperty) throws Exception {
        String className = properties.getProperty(classNameProperty);
        Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
        return (T) clazz.getConstructor().newInstance();
    }

    private static long extractTimestamp(SourceRecord record) {
        if (record.valueSchema().field("ts_ms") == null) {
            return 0L;
        } else {
            return ((Struct) record.value()).getInt64("ts_ms");
        }
    }

    private static ReconnectBehaviour getReconnectBehaviour(Properties properties) {
        String s = (String) properties.get(RECONNECT_BEHAVIOUR_PROPERTY);
        return s == null ? DEFAULT_RECONNECT_BEHAVIOUR : Enum.valueOf(ReconnectBehaviour.class, s.toUpperCase());
    }

    private static long getReconnectIntervalMs(Properties properties) {
        String s = (String) properties.get(RECONNECT_INTERVAL_MS);
        return s == null ? DEFAULT_RECONNECT_INTERVAL_MS : Long.parseLong(s);
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

    public static final class State {

        /**
         * Key represents the partition which the record originated from. Value
         * represents the offset within that partition. Kafka Connect represents
         * the partition and offset as arbitrary values so that is why it is
         * stored as map.
         * See {@link SourceRecord} for more information regarding the format.
         */
        private final Map<Map<String, ?>, Map<String, ?>> partitionsToOffset;

        /**
         * We use a copy-on-write-list because it will be written on a
         * different thread (some internal Debezium snapshot thread) than
         * is normally used to run the connector (one of Jet's blocking
         * worker threads).
         * <p>
         * The performance penalty of copying the list is also acceptable
         * since this list will be written rarely after the initial snapshot,
         * only on table schema changes.
         */
        private final List<byte[]> historyRecords;

        State() {
            this(new HashMap<>(), new CopyOnWriteArrayList<>());
        }

        State(Map<Map<String, ?>, Map<String, ?>> partitionsToOffset, CopyOnWriteArrayList<byte[]> historyRecords) {
            this.partitionsToOffset = partitionsToOffset;
            this.historyRecords = historyRecords;
        }

        public Map<String, ?> getOffset(Map<String, ?> partition) {
            return partitionsToOffset.get(partition);
        }

        public void setOffset(Map<String, ?> partition, Map<String, ?> offset) {
            partitionsToOffset.put(partition, offset);
        }

        Map<Map<String, ?>, Map<String, ?>> getPartitionsToOffset() {
            return partitionsToOffset;
        }

        List<byte[]> getHistoryRecords() {
            return historyRecords;
        }
    }

    public static class DatabaseHistoryImpl extends AbstractDatabaseHistory {

        private final List<byte[]> history;

        public DatabaseHistoryImpl() {
            this.history = Objects.requireNonNull(THREAD_LOCAL_HISTORY.get());
        }

        @Override
        protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
            history.add(DocumentWriter.defaultWriter().writeAsBytes(record.document()));
        }

        @Override
        protected void recoverRecords(Consumer<HistoryRecord> consumer) {
            try {
                for (byte[] record : history) {
                    Document doc = DocumentReader.defaultReader().read(record);
                    consumer.accept(new HistoryRecord(doc));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean exists() {
            return history != null && !history.isEmpty();
        }

        @Override
        public boolean storageExists() {
            return history != null;
        }
    }

    public enum ReconnectBehaviour {

        FAIL,
        CLEAR_STATE_AND_RECONNECT,
        RECONNECT;

    }
}
