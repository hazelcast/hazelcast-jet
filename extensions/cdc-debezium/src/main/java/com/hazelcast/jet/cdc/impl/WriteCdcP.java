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
import com.hazelcast.jet.cdc.CdcSinks;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.impl.connector.UpdateMapWithMaterializedValuesP;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;

//TODO: merge with UpdateMapWithMaterializedValuesP
public class WriteCdcP<K, V> extends UpdateMapWithMaterializedValuesP<ChangeRecord, K, V> {

    private static final int INITIAL_CAPACITY = 4 * 1024;
    private static final float LOAD_FACTOR = 0.75f;

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
        super(instance, map, keyFn, valueFn);

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

    @Override
    protected boolean shouldBeDropped(K key, ChangeRecord item) {
        ChangeRecordImpl recordImpl = (ChangeRecordImpl) item;
        long sequencePartition = recordImpl.getSequencePartition();
        long sequenceValue = recordImpl.getSequenceValue();

        return !updateSequence(key, sequencePartition, sequenceValue);
    }

    /**
     * @param key       key of an event that has just been observed
     * @param partition partition of the source event sequence number
     * @param sequence  numeric value of the source event sequence number
     * @return true if the newly observed sequence number if more
     * recent than what we have observed before (if any)
     */
    boolean updateSequence(K key, long partition, long sequence) {
        Sequence prevSequence = sequences.get(key);
        if (prevSequence == null) { //first observed sequence for key
            sequences.put(key, new Sequence(partition, sequence));
            return true;
        }
        return prevSequence.update(partition, sequence);
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
     * The <i>partition</i> part is a kind of context for the numeric
     * sequence, the "source" of it if you will. It is necessary for
     * avoiding the comparison of numeric sequences which come from
     * different sources.
     */
    private static class Sequence {

        private long timestamp;
        private long partition;
        private long sequence;

        Sequence(long partition, long sequence) {
            this.timestamp = System.currentTimeMillis();
            this.partition = partition;
            this.sequence = sequence;
        }

        boolean isOlderThan(long ageLimitMs) {
            long age = System.currentTimeMillis() - timestamp;
            return age > ageLimitMs;
        }

        boolean update(long partition, long sequence) {
            timestamp = System.currentTimeMillis();

            if (this.partition != partition) { //sequence partition changed for key
                this.partition = partition;
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
