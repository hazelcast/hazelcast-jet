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
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.connector.AbstractHazelcastConnectorSupplier;
import com.hazelcast.jet.impl.connector.UpdateMapWithMaterializedValuesP;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.cdc.Operation.DELETE;
import static com.hazelcast.jet.impl.util.ImdgUtil.asXmlString;

/**
 * Contains factory methods for change data capture specific pipeline
 * sinks. As a consequence these sinks take {@link ChangeRecord} items
 * as their input.
 * <p>
 * All the sink types contained in here are capable to detect any
 * <i>reordering</i> that might have happened in the stream of
 * {@code ChangeRecord} items they ingest. This functionality is
 * optional, but enabled by default (see {@code ignoreReordering} param).
 * It's based on implementation specific sequence numbers provided by
 * CDC event sources. When enabled the sink will selectively drop input
 * items like this:
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
     * Default value for {@code ignoreReordering} parameter. Reordering
     * will <b>NOT</b> be ignored by default.
     */
    public static final boolean IGNORE_REORDERING_DEFAULT_VALUE = false;

    private CdcSinks() {
    }

    /**
     * Returns a sink which maintains an up-to-date image of a change
     * data capture stream in the form of an {@code IMap}. By image we
     * mean that the map should always describe the end result of merging
     * all the change events seen so far.
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
            @Nonnull FunctionEx<ChangeRecord, V> valueFn,
            boolean ignoreReordering
    ) {
        String name = "localMapCdcSink(" + map + ')';
        return sink(name, map, null, keyFn, valueFn, ignoreReordering);
    }

    /**
     * Convenience for {@link #map(String, FunctionEx, FunctionEx, boolean)},
     * reordering detection enabled.
     *
     * @since 4.2
     */
    @Nonnull
    public static <K, V> Sink<ChangeRecord> map(
            @Nonnull String map,
            @Nonnull FunctionEx<ChangeRecord, K> keyFn,
            @Nonnull FunctionEx<ChangeRecord, V> valueFn
    ) {
        return map(map, keyFn, valueFn, IGNORE_REORDERING_DEFAULT_VALUE);
    }

    /**
     * Convenience for {@link #map(String, FunctionEx, FunctionEx, boolean)}
     * with actual {@code IMap} instance being passed in, instead of just
     * name.
     *
     * @since 4.2
     */
    @Nonnull
    public static <K, V> Sink<ChangeRecord> map(
            @Nonnull IMap<? super K, V> map,
            @Nonnull FunctionEx<ChangeRecord, K> keyFn,
            @Nonnull FunctionEx<ChangeRecord, V> valueFn,
            boolean ignoreReordering
    ) {
        return map(map.getName(), keyFn, valueFn, ignoreReordering);
    }

    /**
     * Convenience for {@link #map(String, FunctionEx, FunctionEx)} with
     * actual {@code IMap} instance being passed in, instead of just
     * name, reordering detection enabled.
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
     * Due to the used API, the remote cluster must be at least 3.11.
     *
     * @since 4.2
     */
    @Nonnull
    public static <K, V> Sink<ChangeRecord> remoteMap(
            @Nonnull String map,
            @Nonnull ClientConfig clientConfig,
            @Nonnull FunctionEx<ChangeRecord, K> keyFn,
            @Nonnull FunctionEx<ChangeRecord, V> valueFn,
            boolean ignoreReordering
    ) {
        String name = "remoteMapCdcSink(" + map + ')';
        return sink(name, map, clientConfig, keyFn, valueFn, ignoreReordering);
    }

    /**
     * Convenience for {@link #remoteMap(String, ClientConfig, FunctionEx, FunctionEx, boolean)},
     * reordering detection enabled.
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
        return remoteMap(map, clientConfig, keyFn, valueFn, IGNORE_REORDERING_DEFAULT_VALUE);
    }

    @Nonnull
    private static <K, V> Sink<ChangeRecord> sink(
            @Nonnull String name,
            @Nonnull String map,
            @Nullable ClientConfig clientConfig,
            @Nonnull FunctionEx<ChangeRecord, K> keyFn,
            @Nonnull FunctionEx<ChangeRecord, V> valueFn,
            boolean ignoreReordering
    ) {
        ProcessorSupplier supplier = AbstractHazelcastConnectorSupplier.of(asXmlString(clientConfig),
                instance -> new CdcSinkProcessor<>(instance, map, keyFn, extend(valueFn), ignoreReordering));
        ProcessorMetaSupplier metaSupplier = ProcessorMetaSupplier.forceTotalParallelismOne(supplier, name);
        return new SinkImpl<>(name, metaSupplier, true, null);
    }

    @Nonnull
    private static <V> FunctionEx<ChangeRecord, V> extend(@Nonnull FunctionEx<ChangeRecord, V> valueFn) {
        return (record) -> {
            System.err.println("record = " + record); //todo: remove
            if (DELETE.equals(record.operation())) {
                return null;
            }
            return valueFn.apply(record);
        };
    }

    private static class CdcSinkProcessor<K, V> extends UpdateMapWithMaterializedValuesP<ChangeRecord, K, V> {

        private final Sequences<K> sequences;

        CdcSinkProcessor(
                @Nonnull HazelcastInstance instance,
                @Nonnull String map,
                @Nonnull FunctionEx<? super ChangeRecord, ? extends K> keyFn,
                @Nonnull FunctionEx<? super ChangeRecord, ? extends V> valueFn,
                boolean ignoreReordering
        ) {
            super(instance, map, keyFn, valueFn);
            sequences = ignoreReordering ? null : new Sequences<>();
        }

        @Override
        protected boolean shouldBeDropped(K key, ChangeRecord item) {
            if (sequences == null) {
                return false;
            }

            ChangeRecordImpl recordImpl = (ChangeRecordImpl) item;
            long sequencePartition = recordImpl.getSequencePartition();
            long sequenceValue = recordImpl.getSequenceValue();

            boolean isNew = sequences.update(key, sequencePartition, sequenceValue);
            return !isNew;
        }

    }

    private static final class Sequences<K> {

        private final Map<K, Sequence> sequences = new HashMap<>();

        public boolean update(K key, long partition, long value) {
            Sequence prevSequence = sequences.get(key);
            if (prevSequence == null) { //first observed sequence for key
                sequences.put(key, new Sequence(partition, value));
                return true;
            } else {
                if (prevSequence.partition != partition) { //sequence partition changed for key
                    prevSequence.partition = partition;
                    prevSequence.value = value;
                    return true;
                } else {
                    if (prevSequence.value < value) { //sequence is newer than previous for key
                        prevSequence.value = value;
                        return true;
                    } else {
                        return false;
                    }
                }
            }
        }
    }

    private static final class Sequence {

        private long partition;
        private long value;

        Sequence(long partition, long value) {
            this.partition = partition;
            this.value = value;
        }
    }
}
