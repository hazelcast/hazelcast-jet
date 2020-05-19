/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cdc;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.connector.UpdateMapWithMaterializedValuesP;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.jet.cdc.Operation.DELETE;
import static com.hazelcast.jet.impl.util.ImdgUtil.asXmlString;

/**
 * Contains factory methods for change data capture specific pipeline
 * sinks. As a consequence these sinks take {@link ChangeRecord} items
 * as their input.
 *
 * @since 4.2
 */
public final class CdcSinks {

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
            @Nonnull FunctionEx<ChangeRecord, V> valueFn
    ) {
        String name = "localMapCdcSink(" + map + ')';
        return sink(name, map, null, keyFn, valueFn);
    }

    /**
     * Convenience for {@link #map(String, FunctionEx, FunctionEx)} with
     * actual {@code IMap} instance being passed in, instead of just
     * name.
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
            @Nonnull FunctionEx<ChangeRecord, V> valueFn) {
        ProcessorSupplier supplier = new UpdateMapWithMaterializedValuesP.Supplier<>(
                asXmlString(clientConfig), map, keyFn, extend(valueFn));
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


}
