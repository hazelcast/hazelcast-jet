/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector.kafka;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.SnapshotRestorePolicy;
import com.hazelcast.jet.SnapshotStorage;
import com.hazelcast.jet.Snapshottable;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.impl.util.Util;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static java.util.stream.Collectors.toList;

/**
 * See {@link com.hazelcast.jet.processor.KafkaProcessors#streamKafka(
 * Properties, String...)}.
 */
public final class StreamKafkaP extends AbstractProcessor implements Snapshottable, Closeable {

    private static final int POLL_TIMEOUT_MS = 1000;

    private final Properties properties;
    private final String[] topicIds;
    private CompletableFuture<Void> jobFuture;
    private boolean snapshottingEnabled;
    private KafkaConsumer<?, ?> consumer;

    private final Map<TopicPartition, Long> offsets = new HashMap<>();
    private Traverser<Entry<TopicPartition, Long>> snapshotTraverser;

    public StreamKafkaP(Properties properties, String[] topicIds) {
        this.properties = properties;
        this.topicIds = Arrays.copyOf(topicIds, topicIds.length);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        jobFuture = context.jobFuture();
        snapshottingEnabled = context.snapshottingEnabled();
        consumer = new KafkaConsumer<>(properties);
        // TODO use manual partition assignment
        // TODO handle changing kafka partitions at runtime
        consumer.subscribe(Arrays.asList(topicIds));
    }

    @Override
    public boolean complete() {
        ConsumerRecords<?, ?> records = consumer.poll(POLL_TIMEOUT_MS);

        for (ConsumerRecord<?, ?> r : records) {
            if (snapshottingEnabled) {
                offsets.put(new TopicPartition(r.topic(), r.partition()), r.offset());
            }
            emit(entry(r.key(), r.value()));
        }
        if (!snapshottingEnabled) {
            consumer.commitSync();
        }

        return false;
    }

    @Override
    public void close() {
        consumer.close();
    }

    @Override
    public boolean isCooperative() {
        return false;
    }


    @Nonnull @Override
    public SnapshotRestorePolicy restorePolicy() {
        return SnapshotRestorePolicy.BROADCAST;
    }

    @Override
    public boolean saveSnapshot(SnapshotStorage storage) {
        if (snapshotTraverser == null) {
            snapshotTraverser = Traversers.traverseIterable(offsets.entrySet())
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitSnapshotFromTraverser(storage, snapshotTraverser);
    }

    @Override
    public void restoreSnapshotKey(Object key, Object value) {
        consumer.seek((TopicPartition) key, (Long) value);
    }

    /**
     * Please use {@link com.hazelcast.jet.processor.KafkaProcessors#streamKafka(Properties, String...)}.
     */
    public static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String[] topicIds;
        private final Properties properties;
        private transient List<StreamKafkaP> processors;

        public Supplier(Properties properties, String[] topicIds) {
            this.properties = properties;
            this.topicIds = topicIds;
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            processors = IntStream.range(0, count)
                                         .mapToObj(i -> new StreamKafkaP(properties, topicIds))
                                         .collect(toList());
            return (List) processors;
        }

        @Override
        public void complete(Throwable error) {
            processors.forEach(p -> Util.uncheckRun(p::close));
        }
    }
}
