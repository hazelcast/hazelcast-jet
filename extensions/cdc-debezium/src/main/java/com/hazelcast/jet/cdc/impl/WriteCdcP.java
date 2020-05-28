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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.cdc.CdcSinks;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.impl.connector.AbstractUpdateMapP;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;

public class WriteCdcP<K, V> extends AbstractUpdateMapP<ChangeRecord, K, V> {

    private static final int INITIAL_CAPACITY = 4 * 1024;
    private static final float LOAD_FACTOR = 0.75f;

    private final FunctionEx<? super ChangeRecord, ? extends V> valueFn;

    /**
     * Tracks the last seen sequence for a set of keys.
     * <p>
     * Storing the sequences happens in a LRU cache style, keys that
     * haven't been updated nor read since a certain time (see
     * {@code expirationMs} parameter) will be evicted. This way
     * memory consumption is limited and reordering detection can still
     * be done, since events that have the potential to get their order
     * broken are spaced close to each other in time.
     */
    private LinkedHashMap<K, Sequence> sequences;

    public WriteCdcP(
            @Nonnull HazelcastInstance instance,
            @Nonnull String map,
            @Nonnull FunctionEx<? super ChangeRecord, ? extends K> keyFn,
            @Nonnull FunctionEx<? super ChangeRecord, ? extends V> valueFn
    ) {
        super(instance, MAX_PARALLEL_ASYNC_OPS_DEFAULT, map, keyFn);
        this.valueFn = valueFn;

    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        super.init(outbox, context);

        HazelcastProperties properties = new HazelcastProperties(context.jetInstance().getConfig().getProperties());
        long expirationMs = properties.getMillis(CdcSinks.SEQUENCE_CACHE_EXPIRATION_SECONDS);
        sequences = new LinkedHashMap<K, Sequence>(INITIAL_CAPACITY, LOAD_FACTOR, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, Sequence> eldest) {
                return eldest.getValue().isOlderThan(expirationMs);
            }
        };
    }

    /**
     * @param key       key of an event that has just been observed
     * @param source    source of the event sequence number
     * @param sequence  numeric value of the event sequence number
     * @return true if the newly observed sequence number if more
     * recent than what we have observed before (if any)
     */
    boolean updateSequence(K key, long source, long sequence) {
        Sequence prevSequence = sequences.get(key);
        if (prevSequence == null) { // first observed sequence for key
            sequences.put(key, new Sequence(source, sequence));
            return true;
        }
        return prevSequence.update(source, sequence);
    }

    @Override
    protected void addToBuffer(com.hazelcast.jet.cdc.ChangeRecord item) {
        K key = keyFn.apply(item);
        boolean shouldBeDropped = shouldBeDropped(key, item);
        if (shouldBeDropped) {
            pendingItemCount--;
            return;
        }

        Data keyData = serializationContext.toKeyData(key);
        int partitionId = serializationContext.partitionId(keyData);

        Map<Data, Object> buffer = partitionBuffers[partitionId];
        Data value = serializationContext.toData(valueFn.apply(item));
        if (buffer.put(keyData, value) == null) {
            pendingInPartition[partitionId]++;
        } else { // item already exists, it will be coalesced
            pendingItemCount--;
        }
    }

    private boolean shouldBeDropped(K key, ChangeRecord item) {
        ChangeRecordImpl recordImpl = (ChangeRecordImpl) item;
        long sequenceSource = recordImpl.getSequenceSource();
        long sequenceValue = recordImpl.getSequenceValue();

        return !updateSequence(key, sequenceSource, sequenceValue);
    }

    @Override
    protected EntryProcessor<K, V, Void> entryProcessor(Map<Data, Object> buffer) {
        return new ApplyValuesEntryProcessor<>(buffer);
    }

    /**
     * Tracks a CDC event's sequence number and the moment in time when
     * it was seen.
     * <p>
     * The timestamp is simply the system time taken when a sink
     * instance sees an event.
     * <p>
     * The sequence numbers originate from Debezium event headers and
     * consist of two parts.
     * <p>
     * The <i>sequence</i> part is exactly what the name implies: a
     * numeric value which we can base ordering on.
     * <p>
     * The <i>source</i> part is a kind of context for the numeric
     * sequence. It is necessary for avoiding the comparison of numeric
     * sequences which come from different sources.
     */
    private static class Sequence {

        private long timestamp;
        private long source;
        private long sequence;

        Sequence(long source, long sequence) {
            this.timestamp = System.currentTimeMillis();
            this.source = source;
            this.sequence = sequence;
        }

        boolean isOlderThan(long ageLimitMs) {
            long age = System.currentTimeMillis() - timestamp;
            return age > ageLimitMs;
        }

        boolean update(long source, long sequence) {
            timestamp = System.currentTimeMillis();

            if (this.source != source) { //sequence source changed for key
                this.source = source;
                this.sequence = sequence;
                return true;
            }

            if (this.sequence < sequence) { //sequence is newer than previous for key
                this.sequence = sequence;
                return true;
            }

            return false;
        }
    }
}
