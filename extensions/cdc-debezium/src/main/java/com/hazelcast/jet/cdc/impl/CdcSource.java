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
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public abstract class CdcSource<T> {

    public static final String CONNECTOR_CLASS_PROPERTY = "connector.class";
    public static final String SEQUENCE_EXTRACTOR_CLASS_PROPERTY = "sequence.extractor.class";
    public static final String RECONNECT_INTERVAL_MS = "reconnect.interval.ms";
    public static final String RECONNECT_BEHAVIOUR_PROPERTY = "reconnect.behaviour";

    public static final long DEFAULT_RECONNECT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(5);
    public static final ReconnectBehaviour DEFAULT_RECONNECT_BEHAVIOUR = ReconnectBehaviour.FAIL;

    private static final ThreadLocal<List<byte[]>> THREAD_LOCAL_HISTORY = new ThreadLocal<>();

    private final ILogger logger;

    private final Properties properties;

    private final long reconnectIntervalNanos;
    private final ReconnectBehaviour reconnectBehaviour;

    private State state = new State();
    private SourceConnector connector;
    private Map<String, String> taskConfig;
    private SourceTask task;
    private Long sleepUntilNanoTime;

    CdcSource(Processor.Context context, Properties properties) {
        this.logger = context.logger();
        this.properties = properties;
        reconnectIntervalNanos = getReconnectIntervalNanos(properties);
        reconnectBehaviour = getReconnectBehaviour(properties);
    }

    public void destroy() {
        task.stop();
        task = null;
        connector.stop();
        connector = null;
    }

    public void fillBuffer(SourceBuilder.TimestampedSourceBuffer<T> buf) {
        if (isSleepNeeded() || !isConnectionUp()) {
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
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private boolean isSleepNeeded() {
        if (sleepUntilNanoTime != null) {
            if (System.nanoTime() < sleepUntilNanoTime) {
                return true;
            }
            sleepUntilNanoTime = null;
        }
        return false;
    }

    private boolean isConnectionUp() {
        try {
            if (connector == null) {
                connector = startNewConnector();
                taskConfig = connector.taskConfigs(1).get(0);
            }
            if (task == null) {
                task = startNewTask();
            }
            return true;
        } catch (ConnectException ce) {
            switch (reconnectBehaviour) {
                case FAIL:
                    throw new JetException("Connecting to database failed" + getCause(ce));
                case RECONNECT:
                case CLEAR_STATE_AND_RECONNECT:
                    logger.warning("Initializing connector task failed, retrying in " +
                            NANOSECONDS.toMillis(reconnectIntervalNanos) + "ms" + getCause(ce));
                    sleepUntilNanoTime = System.nanoTime() + reconnectIntervalNanos;
                    break;
                default:
                    throw new RuntimeException("Programming error, unhandled reconnect behaviour: " + reconnectBehaviour);
            }
            return false;
        } catch (JetException je) {
            throw je;
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private SourceConnector startNewConnector() {
        SourceConnector connector = newInstance(properties.getProperty(CONNECTOR_CLASS_PROPERTY), "connector");
        connector.initialize(new JetConnectorContext());
        connector.start((Map) properties);
        return connector;
    }

    private SourceTask startNewTask() {
        SourceTask task = newInstance(connector.taskClass().getName(), "task");
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

    private void reconnect(ReconnectBehaviour behaviour, ConnectException ce) {
        switch (behaviour) {
            case FAIL:
                throw new JetException("Connection to database lost" + getCause(ce));
            case CLEAR_STATE_AND_RECONNECT:
                logger.warning("Connection to database lost, will attempt to reconnect and retry operations from " +
                        "scratch" + getCause(ce));

                destroy();
                state = new State();
                return;
            case RECONNECT:
                logger.warning("Connection to database lost, will attempt to reconnect and resume operations" +
                        getCause(ce));

                destroy();
                return;
            default:
                throw new RuntimeException("Programming error, unhandled reconnect behaviour: " + behaviour);
        }
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

    public State createSnapshot() {
        return state;
    }

    public void restoreSnapshot(List<State> snapshots) {
        this.state = snapshots.get(0);
    }

    protected abstract T mapToOutput(SourceRecord record);

    private static String getCause(ConnectException ce) {
        StringBuilder sb = new StringBuilder();
        if (ce.getMessage() != null) {
            sb.append(" : ").append(ce.getMessage());
        }
        if (ce.getCause() != null && ce.getCause().getMessage() != null) {
            sb.append(" : ").append(ce.getCause().getMessage());
        }
        return sb.toString();
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

    protected static <T> T newInstance(String className, String type) throws JetException {
        try {
            Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
            return (T) clazz.getConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            throw new JetException(String.format("%s class %s not found", type, className));
        } catch (InstantiationException e) {
            throw new JetException(String.format("%s class %s can't be instantiated", type, className));
        } catch (IllegalArgumentException | NoSuchMethodException e) {
            throw new JetException(String.format("%s class %s has no default constructor", type, className));
        } catch (IllegalAccessException e) {
            throw new JetException(String.format("Default constructor of %s class %s is not accessible", type, className));
        } catch (InvocationTargetException e) {
            throw new JetException(
                    String.format("%s class %s failed on construction: %s", type, className, e.getMessage()));
        }
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

    private static long getReconnectIntervalNanos(Properties properties) {
        String s = (String) properties.get(RECONNECT_INTERVAL_MS);
        return TimeUnit.MILLISECONDS.toNanos(s == null ? DEFAULT_RECONNECT_INTERVAL_MS : Long.parseLong(s));
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
        RECONNECT

    }
}
