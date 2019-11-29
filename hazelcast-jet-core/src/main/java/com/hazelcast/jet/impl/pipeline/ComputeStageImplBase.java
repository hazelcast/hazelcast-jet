/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BiPredicateEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.impl.pipeline.transform.AbstractTransform;
import com.hazelcast.jet.impl.pipeline.transform.FlatMapStatefulTransform;
import com.hazelcast.jet.impl.pipeline.transform.FlatMapTransform;
import com.hazelcast.jet.impl.pipeline.transform.GlobalFlatMapStatefulTransform;
import com.hazelcast.jet.impl.pipeline.transform.GlobalMapStatefulTransform;
import com.hazelcast.jet.impl.pipeline.transform.HashJoinTransform;
import com.hazelcast.jet.impl.pipeline.transform.MapStatefulTransform;
import com.hazelcast.jet.impl.pipeline.transform.MapTransform;
import com.hazelcast.jet.impl.pipeline.transform.MergeTransform;
import com.hazelcast.jet.impl.pipeline.transform.PeekTransform;
import com.hazelcast.jet.impl.pipeline.transform.RebalanceTransform;
import com.hazelcast.jet.impl.pipeline.transform.SinkTransform;
import com.hazelcast.jet.impl.pipeline.transform.TimestampTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkStage;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.core.EventTimePolicy.DEFAULT_IDLE_TIMEOUT;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.impl.pipeline.JetEventFunctionAdapter.FN_ADAPTER;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.filterUsingServicePartitionedTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.flatMapUsingServiceAsyncPartitionedTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.flatMapUsingServicePartitionedTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.mapUsingServicePartitionedTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.partitionedCustomProcessorTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.customProcessorTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.filterUsingServiceTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.flatMapUsingServiceAsyncTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.flatMapUsingServiceTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.mapUsingServiceTransform;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class ComputeStageImplBase<T> extends AbstractStage {

    ComputeStageImplBase(
            @Nonnull Transform transform,
            @Nonnull PipelineImpl pipelineImpl,
            boolean acceptsDownstream
    ) {
        super(transform, acceptsDownstream, pipelineImpl);
    }

    @Nonnull
    public StreamStage<T> addTimestamps(@Nonnull ToLongFunctionEx<? super T> timestampFn, long allowedLateness) {
        checkSerializable(timestampFn, "timestampFn");
        TimestampTransform<JetEvent<T>> tsTransform = new TimestampTransform<>(transform,
                EventTimePolicy.<JetEvent<T>>eventTimePolicy(
                        FN_ADAPTER.adaptTimestampFn(timestampFn),
                        JetEvent::jetEvent,
                        limitingLag(allowedLateness),
                        0, 0, DEFAULT_IDLE_TIMEOUT
                ));
        pipelineImpl.connect(transform, tsTransform);
        return new StreamStageImpl<>(tsTransform, pipelineImpl);
    }

    @SuppressWarnings("unchecked")
    <RET> RET attachRebalance(boolean global) {
        return (RET) attach(new RebalanceTransform<>(this.transform, global));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <R, RET> RET attachMap(@Nonnull FunctionEx<? super T, ? extends R> mapFn) {
        checkSerializable(mapFn, "mapFn");
        return (RET) attach(new MapTransform("map", this.transform, FN_ADAPTER.adaptMapFn(mapFn)));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <RET> RET attachFilter(@Nonnull PredicateEx<T> filterFn) {
        checkSerializable(filterFn, "filterFn");
        PredicateEx adaptedFn = FN_ADAPTER.adaptFilterFn(filterFn);
        return (RET) attach(new MapTransform<T, T>("filter", transform, t -> adaptedFn.test(t) ? t : null));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <R, RET> RET attachFlatMap(
            @Nonnull FunctionEx<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        checkSerializable(flatMapFn, "flatMapFn");
        return (RET) attach(new FlatMapTransform("flat-map", transform, FN_ADAPTER.adaptFlatMapFn(flatMapFn)));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, R, RET> RET attachGlobalMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        checkSerializable(createFn, "createFn");
        checkSerializable(mapFn, "mapFn");
        GlobalMapStatefulTransform<T, S, R> transform = new GlobalMapStatefulTransform(
                this.transform,
                FN_ADAPTER.adaptTimestampFn(),
                createFn,
                FN_ADAPTER.<S, Object, T, R>adaptStatefulMapFn((s, k, t) -> mapFn.apply(s, t))
        );
        return (RET) attach(transform);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, R, RET> RET attachGlobalFlatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        checkSerializable(createFn, "createFn");
        checkSerializable(flatMapFn, "flatMapFn");
        GlobalFlatMapStatefulTransform<T, S, R> transform = new GlobalFlatMapStatefulTransform(
                this.transform,
                FN_ADAPTER.adaptTimestampFn(),
                createFn,
                FN_ADAPTER.<S, Object, T, R>adaptStatefulFlatMapFn((s, k, t) -> flatMapFn.apply(s, t))
        );
        return (RET) attach(transform);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <K, S, R, RET> RET attachMapStateful(
            long ttl,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends R> onEvictFn
    ) {
        checkSerializable(keyFn, "keyFn");
        checkSerializable(createFn, "createFn");
        checkSerializable(mapFn, "mapFn");
        MapStatefulTransform<T, K, S, R> transform = new MapStatefulTransform(
                this.transform,
                ttl,
                FN_ADAPTER.adaptKeyFn(keyFn),
                FN_ADAPTER.adaptTimestampFn(),
                createFn,
                FN_ADAPTER.adaptStatefulMapFn(mapFn),
                onEvictFn != null ? FN_ADAPTER.adaptOnEvictFn(onEvictFn) : null);
        return (RET) attach(transform);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <K, S, R, RET> RET attachFlatMapStateful(
            long ttl,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn
    ) {
        checkSerializable(keyFn, "keyFn");
        checkSerializable(createFn, "createFn");
        checkSerializable(flatMapFn, "mapFn");
        FlatMapStatefulTransform<T, K, S, R> transform = new FlatMapStatefulTransform(
                this.transform,
                ttl,
                FN_ADAPTER.adaptKeyFn(keyFn),
                FN_ADAPTER.adaptTimestampFn(),
                createFn,
                FN_ADAPTER.adaptStatefulFlatMapFn(flatMapFn),
                onEvictFn != null ? FN_ADAPTER.adaptOnEvictFlatMapFn(onEvictFn) : null);
        return (RET) attach(transform);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, R, RET> RET attachMapUsingService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        checkSerializable(mapFn, "mapFn");
        BiFunctionEx adaptedMapFn = FN_ADAPTER.adaptMapUsingServiceFn(mapFn);
        return (RET) attach(
                mapUsingServiceTransform(transform, serviceFactory, adaptedMapFn)
        );
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, RET> RET attachFilterUsingService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        checkSerializable(filterFn, "filterFn");
        BiPredicateEx adaptedFilterFn = FN_ADAPTER.adaptFilterUsingServiceFn(filterFn);
        return (RET) attach(
                filterUsingServiceTransform(transform, serviceFactory, adaptedFilterFn)
        );
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, R, RET> RET attachFlatMapUsingService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        checkSerializable(flatMapFn, "flatMapFn");
        BiFunctionEx adaptedFlatMapFn = FN_ADAPTER.adaptFlatMapUsingServiceFn(flatMapFn);
        return (RET) attach(
                flatMapUsingServiceTransform(transform, serviceFactory, adaptedFlatMapFn)
        );
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, R, RET> RET attachFlatMapUsingServiceAsync(
            @Nonnull String operationName,
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        checkSerializable(flatMapAsyncFn, operationName + "AsyncFn");
        BiFunctionEx adaptedFlatMapFn = FN_ADAPTER.adaptFlatMapUsingServiceAsyncFn(flatMapAsyncFn);
        return (RET) attach(
                flatMapUsingServiceAsyncTransform(transform, operationName, serviceFactory, adaptedFlatMapFn)
        );
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, K, R, RET> RET attachMapUsingPartitionedService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        checkSerializable(mapFn, "mapFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        BiFunctionEx adaptedMapFn = FN_ADAPTER.adaptMapUsingServiceFn(mapFn);
        FunctionEx adaptedPartitionKeyFn = FN_ADAPTER.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                mapUsingServicePartitionedTransform(transform, serviceFactory, adaptedMapFn, adaptedPartitionKeyFn)
        );
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, K, RET> RET attachFilterUsingPartitionedService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        checkSerializable(filterFn, "filterFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        BiPredicateEx adaptedFilterFn = FN_ADAPTER.adaptFilterUsingServiceFn(filterFn);
        FunctionEx adaptedPartitionKeyFn = FN_ADAPTER.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                filterUsingServicePartitionedTransform(
                        transform, serviceFactory, adaptedFilterFn, adaptedPartitionKeyFn)
        );
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, K, R, RET> RET attachFlatMapUsingPartitionedService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        checkSerializable(flatMapFn, "flatMapFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        BiFunctionEx adaptedFlatMapFn = FN_ADAPTER.adaptFlatMapUsingServiceFn(flatMapFn);
        FunctionEx adaptedPartitionKeyFn = FN_ADAPTER.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                flatMapUsingServicePartitionedTransform(
                        transform, serviceFactory, adaptedFlatMapFn, adaptedPartitionKeyFn)
        );
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, K, R, RET> RET attachTransformUsingPartitionedServiceAsync(
            @Nonnull String operationName,
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiFunctionEx<? super S, ? super T, CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        checkSerializable(flatMapAsyncFn, operationName + "AsyncFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        BiFunctionEx adaptedFlatMapFn = FN_ADAPTER.adaptFlatMapUsingServiceAsyncFn(flatMapAsyncFn);
        FunctionEx adaptedPartitionKeyFn = FN_ADAPTER.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                flatMapUsingServiceAsyncPartitionedTransform(
                        transform, operationName, serviceFactory, adaptedFlatMapFn, adaptedPartitionKeyFn)
        );
    }

    @Nonnull
    <RET> RET attachMerge(@Nonnull GeneralStage<? extends T> other) {
        return attach(new MergeTransform<>(transform, ((AbstractStage) other).transform));
    }

    @Nonnull
    <K1, T1_IN, T1, R, RET> RET attachHashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause,
            @Nonnull BiFunctionEx<T, T1, R> mapToOutputFn
    ) {
        checkSerializable(mapToOutputFn, "mapToOutputFn");
        return attach(new HashJoinTransform<>(
                asList(transform, transformOf(stage1)),
                singletonList(FN_ADAPTER.adaptJoinClause(joinClause)),
                emptyList(),
                FN_ADAPTER.adaptHashJoinOutputFn(mapToOutputFn)
        ));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <K1, T1_IN, T1, K2, T2_IN, T2, R, TA, RET> RET attachHashJoin2(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull TriFunction<T, T1, T2, R> mapToOutputFn
    ) {
        checkSerializable(mapToOutputFn, "mapToOutputFn");
        return attach(new HashJoinTransform(
                asList(transform, transformOf(stage1), transformOf(stage2)),
                asList(FN_ADAPTER.adaptJoinClause(joinClause1), FN_ADAPTER.adaptJoinClause(joinClause2)),
                emptyList(),
                FN_ADAPTER.adaptHashJoinOutputFn(mapToOutputFn)
        ));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <RET> RET attachPeek(
            @Nonnull PredicateEx<? super T> shouldLogFn,
            @Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn
    ) {
        checkSerializable(shouldLogFn, "shouldLogFn");
        checkSerializable(toStringFn, "toStringFn");
        return attach(new PeekTransform(transform, FN_ADAPTER.adaptFilterFn(shouldLogFn),
                FN_ADAPTER.adaptToStringFn(toStringFn)
        ));
    }

    @Nonnull
    <RET> RET attachCustomTransform(
            @Nonnull String stageName,
            @Nonnull ProcessorMetaSupplier procSupplier
    ) {
        return attach(customProcessorTransform(stageName, transform, procSupplier));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <K, RET> RET attachPartitionedCustomTransform(
            @Nonnull String stageName,
            @Nonnull ProcessorMetaSupplier procSupplier,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn
    ) {
        FunctionEx adaptedKeyFn = FN_ADAPTER.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                partitionedCustomProcessorTransform(stageName, transform, procSupplier, adaptedKeyFn)
        );
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public SinkStage writeTo(@Nonnull Sink<? super T> sink) {
        SinkImpl sinkImpl = (SinkImpl) sink;
        SinkTransform<T> sinkTransform = new SinkTransform(sinkImpl, transform);
        SinkStageImpl output = new SinkStageImpl(sinkTransform, pipelineImpl);
        sinkImpl.onAssignToStage();
        pipelineImpl.connect(transform, sinkTransform);
        return output;
    }

    @Nonnull
    abstract <RET> RET attach(@Nonnull AbstractTransform transform);
}
