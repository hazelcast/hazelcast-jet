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

package com.hazelcast.jet.cdc;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.cdc.impl.ChangeRecordImpl;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.connector.AbstractHazelcastConnectorSupplier;
import com.hazelcast.jet.impl.connector.UpdateMapWithMaterializedValuesP;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.hazelcast.jet.cdc.Operation.DELETE;
import static com.hazelcast.jet.impl.util.ImdgUtil.asXmlString;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Contains factory methods for change data capture specific pipeline
 * sinks. As a consequence these sinks take {@link ChangeRecord} items
 * as their input.
 * <p>
 * These sinks can detect any <i>reordering</i> that might have happened
 * in the stream of {@code ChangeRecord} items they ingest (Jet pipelines
 * benefit from massively parallel execution, so item reordering can and
 * does happen). The reordering is based on implementation specific
 * sequence numbers provided by CDC event sources. The sink reacts to
 * reordering by dropping obsolete input items. The exact behaviour
 * looks like this. For each input item the sink:
 * <ol>
 *  <li>applies the {@code keyFn} on the input item to extract its key</li>
 *  <li>extracts the input item's sequence number</li>
 *  <li>compares the extracted sequence number against the previously
 *          seen sequence number for the same key, if any</li>
 *  <li>if there is a previously seen sequence number and is more recent
 *          than the one observed in the input item, then drops (ignores)
 *          the input item</li>
 * </ol>
 *
 * @since 4.2
 */
public final class CdcSinks {

    /**
     * Number of seconds for which the last seen sequence number for any
     * input key will be guarantied to be remembered (used for
     * reordering detection). After this time, the last seen sequence
     * number values will eventually be evicted, in order to save space.
     * <p>
     * Default value is 10 seconds.
     *
     * @since 4.2
     */
    public static final HazelcastProperty SEQUENCE_CACHE_EXPIRATION_SECONDS
            = new HazelcastProperty("jet.cdc.sink.sequence.cache.expiration.seconds", 10, SECONDS);

    private CdcSinks() {
    }

    /**
     * Returns a sink which maintains an up-to-date mirror of a change
     * data capture stream in the form of an {@code IMap}. By mirror we
     * mean that the map should always describe the end result of merging
     * all the change events seen so far.
     * <p>
     * <b>NOTE</b>: in order for the sink behaviour to be predictable
     * the map should be non-existent or empty by the time the sink starts
     * using it.
     * <p>
     * For each item the sink receives it uses the {@code keyFn} to
     * determine which map key the change event applies to. Then, based
     * on the {@code ChangeRecord}'s {@code Operation} it decides to
     * either:
     * <ul>
     *   <li>delete the key from the map
     *          ({@link Operation#DELETE})</li>
     *   <li>insert a new value for the key
     *          ({@link Operation#SYNC} & {@link Operation#INSERT})</li>
     *   <li>update the current value for the key
     *          ({@link Operation#UPDATE})</li>
     * </ul>
     * For insert and update operations the new value to use is
     * determined from the input record by using the provided
     * {@code valueFn}. <strong>IMPORTANT</strong> to note that if the
     * {@code valueFn} returns {@code null}, then the key will be
     * deleted from the map no matter the operation (ie. even for update
     * and insert records).
     * <p>
     * For the functionality of this sink it is vital that the order of
     * the input items is preserved so we'll always create a single
     * instance of it in each pipeline.
     *
     * @since 4.2
     */
    @Nonnull
    public static <K, V> Sink<ChangeRecord> map(
            @Nonnull String map,
            @Nonnull FunctionEx<ChangeRecord, K> keyFn,
            @Nonnull FunctionEx<ChangeRecord, V> valueFn
    ) {
        String name = "localMapCdcSink(" + map + ')';
        return sink(name, map, null, keyFn, valueFn);
    }

    /**
     * Returns a sink which maintains an up-to-date mirror of a change
     * data capture stream in the form of an {@code IMap}. By mirror we
     * mean that the map should always describe the end result of merging
     * all the change events seen so far.
     * <p>
     * <b>NOTE</b>: in order for the sink behaviour to be predictable
     * the map should be non-existent or empty by the time the sink starts
     * using it.
     * <p>
     * For each item the sink receives it uses the {@code keyFn} to
     * determine which map key the change event applies to. Then, based
     * on the {@code ChangeRecord}'s {@code Operation} it decides to
     * either:
     * <ul>
     *   <li>delete the key from the map
     *          ({@link Operation#DELETE})</li>
     *   <li>insert a new value for the key
     *          ({@link Operation#SYNC} & {@link Operation#INSERT})</li>
     *   <li>update the current value for the key
     *          ({@link Operation#UPDATE})</li>
     * </ul>
     * For insert and update operations the new value to use is
     * determined from the input record by using the provided
     * {@code valueFn}. <strong>IMPORTANT</strong> to note that if the
     * {@code valueFn} returns {@code null}, then the key will be
     * deleted from the map no matter the operation (ie. even for update
     * and insert records).
     * <p>
     * For the functionality of this sink it is vital that the order of
     * the input items is preserved so we'll always create a single
     * instance of it in each pipeline.
     *
     * @since 4.2
     */
    @Nonnull
    public static <K, V> Sink<ChangeRecord> map(
            @Nonnull IMap<? super K, V> map,
            @Nonnull FunctionEx<ChangeRecord, K> keyFn,
            @Nonnull FunctionEx<ChangeRecord, V> valueFn
    ) {
        return map(map.getName(), keyFn, valueFn);
    }

    /**
     * Returns a sink equivalent to {@link #map}, but for a map in a
     * remote Hazelcast cluster identified by the supplied {@code
     * ClientConfig}.
     * <p>
     * <b>NOTE</b>: same limitation as for {@link #map}, the map should
     * be non-existent or empty by the time the sink starts using it.
     * <p>
     * Due to the used API, the remote cluster must be at least version 4.0.
     *
     * @since 4.2
     */
    @Nonnull
    public static <K, V> Sink<ChangeRecord> remoteMap(
            @Nonnull String map,
            @Nonnull ClientConfig clientConfig,
            @Nonnull FunctionEx<ChangeRecord, K> keyFn,
            @Nonnull FunctionEx<ChangeRecord, V> valueFn
    ) {
        String name = "remoteMapCdcSink(" + map + ')';
        return sink(name, map, clientConfig, keyFn, valueFn);
    }

    @Nonnull
    private static <K, V> Sink<ChangeRecord> sink(
            @Nonnull String name,
            @Nonnull String map,
            @Nullable ClientConfig clientConfig,
            @Nonnull FunctionEx<ChangeRecord, K> keyFn,
            @Nonnull FunctionEx<ChangeRecord, V> valueFn
    ) {
        ProcessorSupplier supplier = AbstractHazelcastConnectorSupplier.of(asXmlString(clientConfig),
                instance -> new CdcSinkProcessor<>(instance, map, keyFn, extend(valueFn)));
        ProcessorMetaSupplier metaSupplier = ProcessorMetaSupplier.forceTotalParallelismOne(supplier, name);
        return new SinkImpl<>(name, metaSupplier, true, null);
    }

    @Nonnull
    private static <V> FunctionEx<ChangeRecord, V> extend(@Nonnull FunctionEx<ChangeRecord, V> valueFn) {
        return (record) -> {
            if (DELETE.equals(record.operation())) {
                return null;
            }
            return valueFn.apply(record);
        };
    }

    private static class CdcSinkProcessor<K, V> extends UpdateMapWithMaterializedValuesP<ChangeRecord, K, V> {

        private Sequences<K> sequences;

        CdcSinkProcessor(
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
            long expirationMs = properties.getMillis(SEQUENCE_CACHE_EXPIRATION_SECONDS);
            this.sequences = new Sequences<>(expirationMs);
        }

        @Override
        protected boolean shouldBeDropped(K key, ChangeRecord item) {
            ChangeRecordImpl recordImpl = (ChangeRecordImpl) item;
            long sequencePartition = recordImpl.getSequencePartition();
            long sequenceValue = recordImpl.getSequenceValue();

            boolean isNew = sequences.update(key, sequencePartition, sequenceValue);
            return !isNew;
        }

        private static long getTimestamp(ChangeRecord item) {
            try {
                return item.timestamp();
            } catch (ParsingException e) {
                //use current time, should be good enough for cache expiration purposes
                return System.currentTimeMillis();
            }
        }

    }

    /**
     * Tracks the last seen sequence number for a set of keys. The
     * sequence numbers originate from Debezium event headers and consist
     * of two parts.
     * <p>
     * The <i>sequence</i> part is exactly what the name implies: a numeric
     * sequence which we base our ordering on. Implementations needs to ensure
     * that {@code ChangeRecord}s produced by a source contain a monotonic
     * increasing sequence number, as long as the sequnce number partition
     * doesn't change.
     * <p>
     * The <i>partition</i> part is a kind of context for the numeric
     * sequence, the "source" of it if you will. It is necessary for avoiding
     * the comparison of numeric sequences which come from different sources.
     * For example if the numeric sequence is in fact based on transaction
     * IDs, then it makes sense to compare them only if they are produced by
     * the same database instance. Or if the numeric ID is an offset in a
     * write-ahead log, then it makes sense to compare them only if they are
     * offsets from the same log file. Implementations need to make sure that
     * the partition is the same if and only if the source of the numeric
     * sequence is the same.
     * <p>
     * Tracking of the sequence numbers for various keys happens in a
     * LRU cache style, keys that haven't been updated nor read since a
     * certain time (see {@code expirationMs} parameter) will be evicted,
     * to keep the memory consumption limited. This is ok, because
     * reordering of events happens only in case of events spaced very
     * close to each other in time.
     */
    private static final class Sequences<K> {

        private static final int INITIAL_CAPACITY = 4 * 1024;
        private static final float LOAD_FACTOR = 0.75f;

        private final LinkedHashMap<K, long[]> sequences;

        /**
         * @param expirationMs number of milliseconds for which a sequence
         *                     observed for a certain key is guaranteed
         *                     to be tracked; might be evicted afterwards
         */
        Sequences(long expirationMs) {
            sequences = new LinkedHashMap<K, long[]>(INITIAL_CAPACITY, LOAD_FACTOR, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<K, long[]> eldest) {
                    long age = System.currentTimeMillis() - eldest.getValue()[2];
                    return age > expirationMs;
                }
            };
        }

        /**
         * @param key       key of an event that has just been observed
         * @param partition partition of the source event sequence number
         * @param sequence  numeric value of the source event sequence number
         * @return true if the newly observed sequence number if more
         * recent than what we have observed before (if any)
         */
        boolean update(K key, long partition, long sequence) {
            long[] prevSequence = sequences.get(key);
            if (prevSequence == null) { //first observed sequence for key
                sequences.put(key, new long[]{partition, sequence, System.currentTimeMillis()});
                return true;
            } else {
                prevSequence[2] = System.currentTimeMillis();
                if (prevSequence[0] != partition) { //sequence partition changed for key
                    prevSequence[0] = partition;
                    prevSequence[1] = sequence;
                    return true;
                } else {
                    if (prevSequence[1] < sequence) { //sequence is newer than previous for key
                        prevSequence[1] = sequence;
                        return true;
                    } else {
                        return false;
                    }
                }
            }
        }
    }
}
