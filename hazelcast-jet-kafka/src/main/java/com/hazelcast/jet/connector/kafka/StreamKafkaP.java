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

package com.hazelcast.jet.connector.kafka;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.Util.entry;

/**
 * Kafka reader for Jet, emits records read from Kafka as {@code Map.Entry}.
 */
public final class StreamKafkaP extends AbstractProcessor {

    private static final int ZERO_POLL_TIMEOUT = 0;
    private final Properties properties;
    private final String[] topicIds;
    private CompletableFuture<Void> jobFuture;
    private KafkaConsumer<?, ?> consumer;
    private Iterator<? extends ConsumerRecord<?, ?>> iterator;
    private Map.Entry<?, ?> pendingEntry;

    private StreamKafkaP(String[] topicIds, Properties properties) {
        this.topicIds = topicIds;
        this.properties = properties;
    }

    /**
     * Returns a 2supplier of processors that consume one or more Kafka topics and emit
     * items from it as {@code Map.Entry} instances.
     * <p>
     * A {@code KafkaConsumer} is created per {@code Processor} instance using the
     * supplied properties. All processors must be in the same consumer group
     * supplied by the {@code group.id} property.
     * The supplied properties will be passed on to the {@code KafkaConsumer} instance.
     * These processors are only terminated in case of an error or if the underlying job is cancelled.
     *
     * @param topics     the list of topics
     * @param properties consumer properties which should contain consumer group name,
     *                   broker address and key/value deserializers
     */
    public static DistributedSupplier<Processor> streamKafka(Properties properties, String... topics) {
        Preconditions.checkPositive(topics.length, "At least one topic must be supplied");
        Preconditions.checkTrue(properties.containsKey("group.id"), "Properties should contain `group.id`");
        properties.put("enable.auto.commit", false);

        return () -> new StreamKafkaP(topics, properties);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        jobFuture = context.jobFuture();
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topicIds));
    }

    @Override
    public boolean complete() {
        if (pendingEntry != null) {
            // If successfully emit the pending entry, resume the consumer
            // else do an empty poll for heartbeat
            if (tryEmit(pendingEntry)) {
                pendingEntry = null;
                resume();
            } else {
                emptyPoll();
                return jobFuture.isDone();
            }
        }
        // poll for records if iterator is null
        if (iterator == null) {
            iterator = consumer.poll(ZERO_POLL_TIMEOUT).iterator();
        }
        while (iterator.hasNext() && !jobFuture.isDone()) {
            ConsumerRecord<?, ?> record = iterator.next();
            Map.Entry<?, ?> entry = entry(record.key(), record.value());
            // If emit is not successful save the entry as pendingEntry
            // pause the consumer and do an empty poll for heartbeat
            if (!tryEmit(entry)) {
                pendingEntry = entry;
                pause();
                emptyPoll();
                return false;
            }
        }
        // Records are consumed, commit the offset and reset the iterator for next poll
        consumer.commitSync();
        iterator = null;
        return jobFuture.isDone();
    }

    private void pause() {
        consumer.pause(consumer.assignment());
    }

    private void resume() {
        consumer.resume(consumer.assignment());
    }

    private void emptyPoll() {
        int count = consumer.poll(0).count();
        if (count != 0) {
            throw new IllegalStateException("Empty poll call returned some records: " + count);
        }
    }

}
