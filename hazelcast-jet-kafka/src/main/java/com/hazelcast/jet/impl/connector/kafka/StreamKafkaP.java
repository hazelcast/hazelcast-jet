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
import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.SnapshotRestorePolicy;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.nio.Address;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static java.util.stream.Collectors.toList;

/**
 * See {@link com.hazelcast.jet.processor.KafkaProcessors#streamKafka(
 * Properties, String...)}.
 */
public final class StreamKafkaP extends AbstractProcessor implements Closeable {

    private static final int POLL_TIMEOUT_MS = 1000;

    private final Properties properties;
    private final String[] topicIds;
    private final int processorCount;
    private final int processorIndex;
    private CompletableFuture<Void> jobFuture;
    private boolean snapshottingEnabled;
    private KafkaConsumer<?, ?> consumer;

    private final Map<TopicPartition, Long> offsets = new HashMap<>();
    private Traverser<Entry<TopicPartition, Long>> snapshotTraverser;

    StreamKafkaP(Properties properties, String[] topicIds, int processorCount, int processorIndex) {
        this.properties = properties;
        this.topicIds = Arrays.copyOf(topicIds, topicIds.length);
        this.processorCount = processorCount;
        this.processorIndex = processorIndex;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        jobFuture = context.jobFuture();
        snapshottingEnabled = context.snapshottingEnabled();
        consumer = new KafkaConsumer<>(properties);

        List<TopicPartition> assignment = new ArrayList<>();
        for (String topicId : topicIds) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topicId);
            for (int i = processorIndex; i < partitionInfos.size(); i += processorCount) {
                assignment.add(new TopicPartition(topicId, partitionInfos.get(i).partition()));
            }
        }
        consumer.assign(assignment);
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

    @Override
    public boolean saveSnapshot() {
        if (snapshotTraverser == null) {
            snapshotTraverser = Traversers.traverseIterable(offsets.entrySet())
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitSnapshotFromTraverser(snapshotTraverser);
    }

    @Override
    public void restoreSnapshot(@Nonnull Inbox inbox) {
        Set<TopicPartition> assignment = consumer.assignment();
        for (Object o; (o = inbox.poll()) != null; ) {
            Entry<TopicPartition, Long> entry = (Entry<TopicPartition, Long>) o;
            if (assignment.contains(entry.getKey())) {
                consumer.seek(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Please use {@link com.hazelcast.jet.processor.KafkaProcessors#streamKafka(Properties, String...)}.
     */
    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String[] topicIds;
        private final int memberCount;
        private final int memberIndex;
        private final Properties properties;
        private transient List<StreamKafkaP> processors;
        private int localParallelism;

        Supplier(Properties properties, String[] topicIds, int memberCount, int memberIndex) {
            this.properties = properties;
            this.topicIds = topicIds;
            this.memberCount = memberCount;
            this.memberIndex = memberIndex;
        }

        @Override
        public void init(@Nonnull Context context) {
            localParallelism = context.localParallelism();
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            // localParallelism is equal on all members
            processors = IntStream.range(0, count)
                                         .mapToObj(i -> new StreamKafkaP(properties, topicIds,
                                                 memberCount * localParallelism, memberIndex * localParallelism + i))
                                         .collect(toList());
            return (List) processors;
        }

        @Override
        public void complete(Throwable error) {
            processors.forEach(p -> Util.uncheckRun(p::close));
        }
    }

    public static class MetaSupplier implements ProcessorMetaSupplier {

        private final Properties properties;
        private final String[] topicIds;

        public MetaSupplier(Properties properties, String[] topicIds) {
            this.properties = properties;
            this.topicIds = topicIds;
        }

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new Supplier(properties, topicIds, addresses.size(), addresses.indexOf(address));
        }

        @Nonnull
        @Override
        public SnapshotRestorePolicy snapshotRestorePolicy() {
            return SnapshotRestorePolicy.BROADCAST;
        }
    }
}
