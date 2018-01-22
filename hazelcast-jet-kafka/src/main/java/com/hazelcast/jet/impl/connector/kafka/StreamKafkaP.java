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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.CloseableProcessorSupplier;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.core.WatermarkSourceUtil;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.nio.Address;
import com.hazelcast.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static java.lang.System.arraycopy;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * See {@link com.hazelcast.jet.core.processor.KafkaProcessors#streamKafkaP}.
 */
public final class StreamKafkaP<K, V, T> extends AbstractProcessor implements Closeable {

    private static final long METADATA_CHECK_INTERVAL_NANOS = SECONDS.toNanos(5);
    private static final int POLL_TIMEOUT_MS = 50;

    Map<TopicPartition, Integer> currentAssignment = new HashMap<>();
    private final Properties properties;
    private final List<String> topics;
    private final DistributedBiFunction<K, V, T> projectionFn;
    private final int globalParallelism;
    private final WatermarkSourceUtil<T> watermarkSourceUtil;
    private boolean snapshottingEnabled;
    private KafkaConsumer<Object, Object> consumer;
    private final AppendableTraverser<Object> appendableTraverser = new AppendableTraverser<>(2);

    private final int[] partitionCounts;
    private long nextMetadataCheck = Long.MIN_VALUE;

    /**
     * Key: topicName<br>
     * Value: partition offsets, at index I is offset for partition I.<br>
     * Offsets are -1 initially and remain -1 for partitions not assigned to this instance.
     */
    private final Map<String, long[]> offsets = new HashMap<>();
    private Traverser<Entry<BroadcastKey<TopicPartition>, long[]>> snapshotTraverser;
    private int processorIndex;
    private Traverser<Object> traverser;
    private ConsumerRecord<Object, Object> lastEmittedItem;

    StreamKafkaP(
            @Nonnull Properties properties,
            @Nonnull List<String> topics,
            @Nonnull DistributedBiFunction<K, V, T> projectionFn,
            int globalParallelism,
            @Nonnull WatermarkGenerationParams<T> wmGenParams
    ) {
        this.properties = properties;
        this.topics = topics;
        this.projectionFn = projectionFn;
        this.globalParallelism = globalParallelism;

        watermarkSourceUtil = new WatermarkSourceUtil<>(wmGenParams);
        partitionCounts = new int[topics.size()];
    }

    @Override
    protected void init(@Nonnull Context context) {
        processorIndex = context.globalProcessorIndex();
        snapshottingEnabled = context.snapshottingEnabled();
        consumer = new KafkaConsumer<>(properties);
        assignPartitions(false);
    }

    private void assignPartitions(boolean seekToBeginning) {
        if (System.nanoTime() < nextMetadataCheck) {
            return;
        }
        boolean allEqual = true;
        for (int i = 0; i < topics.size(); i++) {
            int newCount = consumer.partitionsFor(topics.get(i)).size();
            allEqual &= partitionCounts[i] == newCount;
            partitionCounts[i] = newCount;
        }
        if (allEqual) {
            return;
        }

        KafkaPartitionAssigner assigner = new KafkaPartitionAssigner(topics, partitionCounts, globalParallelism);
        Set<TopicPartition> newAssignments = assigner.topicPartitionsFor(processorIndex);
        logFinest(getLogger(), "Currently assigned partitions: %s", newAssignments);

        newAssignments.removeAll(currentAssignment.keySet());
        if (!newAssignments.isEmpty()) {
            getLogger().info("Partition assignments changed, added partitions: " + newAssignments);
            for (TopicPartition tp : newAssignments) {
                currentAssignment.put(tp, currentAssignment.size());
            }
            watermarkSourceUtil.increasePartitionCount(currentAssignment.size());
            consumer.assign(currentAssignment.keySet());
            if (seekToBeginning) {
                // for newly detected partitions, we should always seek to the beginning
                consumer.seekToBeginning(newAssignments);
            }
        }

        createOrExtendOffsetsArrays();
        nextMetadataCheck = System.nanoTime() + METADATA_CHECK_INTERVAL_NANOS;
    }

    private void createOrExtendOffsetsArrays() {
        for (int topicIdx = 0; topicIdx < partitionCounts.length; topicIdx++) {
            int newPartitionCount = partitionCounts[topicIdx];
            String topicName = topics.get(topicIdx);
            long[] oldOffsets = offsets.get(topicName);
            if (oldOffsets != null && oldOffsets.length == newPartitionCount) {
                continue;
            }
            long[] newOffsets = new long[newPartitionCount];
            Arrays.fill(newOffsets, -1);
            if (oldOffsets != null) {
                arraycopy(oldOffsets, 0, newOffsets, 0, oldOffsets.length);
            }
            offsets.put(topicName, newOffsets);
        }
    }

    @Override
    public boolean complete() {
        assignPartitions(true);

        if (traverser == null) {
            ConsumerRecords<Object, Object> records = null;
            if (!currentAssignment.isEmpty()) {
                try {
                    records = consumer.poll(POLL_TIMEOUT_MS);
                } catch (InterruptException e) {
                    // note this is Kafka's exception, not Java's
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            if (records == null || records.isEmpty()) {
                Watermark wm = watermarkSourceUtil.handleNoEvent();
                if (wm != null) {
                    appendableTraverser.append(wm);
                    traverser = appendableTraverser;
                }
            } else {
                traverser = traverseIterable(records)
                        .flatMap(r -> {
                            lastEmittedItem = r;
                            T projectedRecord = projectionFn.apply((K) r.key(), (V) r.value());
                            TopicPartition topicPartition = new TopicPartition(lastEmittedItem.topic(),
                                    lastEmittedItem.partition());
                            Watermark wm = watermarkSourceUtil.handleEvent(currentAssignment.get(topicPartition),
                                    projectedRecord);
                            if (wm != null) {
                                appendableTraverser.append(wm);
                            }
                            if (projectedRecord != null) {
                                appendableTraverser.append(projectedRecord);
                            }
                            return appendableTraverser;
                        });
            }
            if (traverser == null) {
                return false;
            }

            traverser = traverser.onFirstNull(() -> traverser = null);
        }

        emitFromTraverser(traverser,
                e -> {
                    if (!(e instanceof Watermark)) {
                        offsets.get(lastEmittedItem.topic())[lastEmittedItem.partition()] = lastEmittedItem.offset();
                    }
                });

        if (!snapshottingEnabled) {
            consumer.commitSync();
        }

        return false;
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean saveToSnapshot() {
        if (snapshotTraverser == null) {
            Stream<Entry<BroadcastKey<TopicPartition>, long[]>> snapshotStream =
                    offsets.entrySet().stream()
                           .flatMap(entry -> IntStream.range(0, entry.getValue().length)
                                  .filter(partition -> entry.getValue()[partition] >= 0)
                                  .mapToObj(partition -> {
                                      TopicPartition key = new TopicPartition(entry.getKey(), partition);
                                      long offset = entry.getValue()[partition];
                                      long watermark = watermarkSourceUtil.getWatermark(currentAssignment.get(key));
                                      return entry(broadcastKey(key), new long[]{offset, watermark});
                                  }));
            snapshotTraverser = traverseStream(snapshotStream)
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        TopicPartition topicPartition = ((BroadcastKey<TopicPartition>) key).key();
        long[] value1 = (long[]) value;
        long offset = value1[0];
        long watermark = value1[1];
        long[] topicOffsets = offsets.get(topicPartition.topic());
        if (topicOffsets == null) {
            getLogger().severe("Offset for topic '" + topicPartition.topic()
                    + "' is present in snapshot, but the topic is not supposed to be read");
            return;
        }
        if (topicPartition.partition() >= topicOffsets.length) {
            getLogger().severe("Offset for partition '" + topicPartition + "' is present in snapshot," +
                    " but that topic currently has only " + topicOffsets.length + " partitions");
        }
        Integer partitionIndex = currentAssignment.get(topicPartition);
        if (partitionIndex != null) {
            assert topicOffsets[topicPartition.partition()] < 0 : "duplicate offset for topicPartition '" + topicPartition
                    + "' restored, offset1=" + topicOffsets[topicPartition.partition()] + ", offset2=" + offset;
            topicOffsets[topicPartition.partition()] = offset;
            consumer.seek(topicPartition, offset + 1);
            watermarkSourceUtil.restoreWatermark(partitionIndex, watermark);
        }
    }

    public static class MetaSupplier<K, V, T> implements ProcessorMetaSupplier {

        private final Properties properties;
        private final List<String> topics;
        private final DistributedBiFunction<K, V, T> projectionFn;
        private final WatermarkGenerationParams<T> wmGenParams;
        private int totalParallelism;

        public MetaSupplier(
                Properties properties,
                List<String> topics,
                DistributedBiFunction<K, V, T> projectionFn,
                @Nonnull WatermarkGenerationParams<T> wmGenParams) {
            this.properties = new Properties();
            this.properties.putAll(properties);
            this.topics = topics;
            this.projectionFn = projectionFn;
            this.wmGenParams = wmGenParams;
        }

        @Override
        public int preferredLocalParallelism() {
            return 2;
        }

        @Override
        public void init(@Nonnull Context context) {
            totalParallelism = context.totalParallelism();
        }

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new CloseableProcessorSupplier<>(
                    () -> new StreamKafkaP(properties, topics, projectionFn, totalParallelism, wmGenParams));
        }
    }

    /**
     * Helper class for assigning partitions to processor indices in a round robin fashion
     */
    static class KafkaPartitionAssigner {

        private final List<String> topics;
        private final int[] partitionCounts;
        private final int globalParallelism;

        KafkaPartitionAssigner(List<String> topics, int[] partitionCounts, int globalParallelism) {
            Preconditions.checkTrue(topics.size() == partitionCounts.length,
                    "Different length between topics and partition counts");
            this.topics = topics;
            this.partitionCounts = partitionCounts;
            this.globalParallelism = globalParallelism;
        }

        Set<TopicPartition> topicPartitionsFor(int processorIndex) {
            Set<TopicPartition> assignments = new LinkedHashSet<>();
            for (int topicIndex = 0; topicIndex < topics.size(); topicIndex++) {
                for (int partition = 0; partition < partitionCounts[topicIndex]; partition++) {
                    if (processorIndexFor(topicIndex, partition) == processorIndex) {
                        assignments.add(new TopicPartition(topics.get(topicIndex), partition));
                    }
                }
            }
            return assignments;
        }

        private int processorIndexFor(int topicIndex, int partition) {
            int startIndex = topicIndex * Math.max(1, globalParallelism / topics.size());
            return (startIndex + partition) % globalParallelism;
        }
    }
}
