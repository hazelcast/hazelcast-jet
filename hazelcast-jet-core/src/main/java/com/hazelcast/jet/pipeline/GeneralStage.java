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

package com.hazelcast.jet.pipeline;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BiPredicateEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.pipeline.ComputeStageImplBase;
import com.hazelcast.jet.impl.processor.AbstractAsyncTransformUsingServiceP;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.hazelcast.function.PredicateEx.alwaysTrue;
import static com.hazelcast.jet.impl.processor.AbstractAsyncTransformUsingServiceP.DEFAULT_MAX_CONCURRENT_OPS;
import static com.hazelcast.jet.impl.processor.AbstractAsyncTransformUsingServiceP.DEFAULT_PRESERVE_ORDER;

/**
 * The common aspect of {@link BatchStage batch} and {@link StreamStage
 * stream} pipeline stages, defining those operations that apply to both.
 * <p>
 * Unless specified otherwise, all functions passed to methods of this
 * interface must be stateless.
 *
 * @param <T> the type of items coming out of this stage
 *
 * @since 3.0
 */
public interface GeneralStage<T> extends Stage {

    /**
     * Attaches a mapping stage which applies the given function to each input
     * item independently and emits the function's result as the output item.
     * If the result is {@code null}, it emits nothing. Therefore this stage
     * can be used to implement filtering semantics as well.
     * <p>
     * This sample takes a stream of names and outputs the names in lowercase:
     * <pre>{@code
     * stage.map(name -> name.toLowerCase())
     * }</pre>
     *
     * @param mapFn a stateless mapping function
     * @param <R> the result type of the mapping function
     * @return the newly attached stage
     */
    @Nonnull
    <R> GeneralStage<R> map(@Nonnull FunctionEx<? super T, ? extends R> mapFn);

    /**
     * Attaches a filtering stage which applies the provided predicate function
     * to each input item to decide whether to pass the item to the output or
     * to discard it. Returns the newly attached stage.
     * <p>
     * This sample removes empty strings from the stream:
     * <pre>{@code
     * stage.filter(name -> !name.isEmpty())
     * }</pre>
     *
     * @param filterFn a stateless filter predicate function
     * @return the newly attached stage
     */
    @Nonnull
    GeneralStage<T> filter(@Nonnull PredicateEx<T> filterFn);

    /**
     * Attaches a flat-mapping stage which applies the supplied function to
     * each input item independently and emits all the items from the {@link
     * Traverser} it returns. The traverser must be <em>null-terminated</em>.
     * <p>
     * This sample takes a stream of sentences and outputs a stream of
     * individual words in them:
     * <pre>{@code
     * stage.map(sentence -> traverseArray(sentence.split("\\W+")))
     * }</pre>
     *
     * @param flatMapFn a stateless flatmapping function, whose result type is
     *                  Jet's {@link Traverser}. It must not return null
     *                  traverser, but can return an {@linkplain
     *                  Traversers#empty() empty traverser}.
     * @param <R> the type of items in the result's traversers
     * @return the newly attached stage
     */
    @Nonnull
    <R> GeneralStage<R> flatMap(
            @Nonnull FunctionEx<? super T, ? extends Traverser<R>> flatMapFn
    );

    /**
     * Attaches a stage that performs a stateful mapping operation. {@code
     * createFn} returns the object that holds the state. Jet passes this
     * object along with each input item to {@code mapFn}, which can update
     * the object's state. The state object will be included in the state
     * snapshot, so it survives job restarts. For this reason it must be
     * serializable.
     * <p>
     * This sample takes a stream of {@code long} numbers representing request
     * latencies, computes the cumulative latency of all requests so far, and
     * starts emitting alarm messages when the cumulative latency crosses a
     * "bad behavior" threshold:
     * <pre>{@code
     * StreamStage<Long> latencyAlarms = latencies.mapStateful(
     *         LongAccumulator::new,
     *         (sum, latency) -> {
     *             sum.add(latency);
     *             long cumulativeLatency = sum.get();
     *             return (cumulativeLatency <= LATENCY_THRESHOLD)
     *                     ? null
     *                     : cumulativeLatency;
     *         }
     * );
     * }</pre>
     * This code has the same result as {@link #rollingAggregate
     * latencies.rollingAggregate(summing())}.
     *
     * @param createFn function that returns the state object
     * @param mapFn    function that receives the state object and the input item and
     *                 outputs the result item. It may modify the state object.
     * @param <S>      type of the state object
     * @param <R>      type of the result
     */
    @Nonnull
    <S, R> GeneralStage<R> mapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    );

    /**
     * Attaches a stage that performs a stateful filtering operation. {@code
     * createFn} returns the object that holds the state. Jet passes this
     * object along with each input item to {@code filterFn}, which can update
     * the object's state. The state object will be included in the state
     * snapshot, so it survives job restarts. For this reason it must be
     * serializable.
     * <p>
     * This sample decimates the input (throws out every 10th item):
     * <pre>{@code
     * GeneralStage<String> decimated = input.filterStateful(
     *         LongAccumulator::new,
     *         (counter, item) -> {
     *             counter.add(1);
     *             return counter.get() % 10 != 0;
     *         }
     * );
     * }</pre>
     *
     * @param createFn function that returns the state object
     * @param filterFn function that receives the state object and the input item and
     *                 produces the boolean result. It may modify the state object.
     * @param <S>      type of the state object
     */
    @Nonnull
    <S> GeneralStage<T> filterStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    );

    /**
     * Attaches a stage that performs a stateful flat-mapping operation. {@code
     * createFn} returns the object that holds the state. Jet passes this
     * object along with each input item to {@code flatMapFn}, which can update
     * the object's state. The state object will be included in the state
     * snapshot, so it survives job restarts. For this reason it must be
     * serializable.
     * <p>
     * This sample inserts a punctuation mark (a special string) after every
     * 10th input string:
     * <pre>{@code
     * GeneralStage<String> punctuated = input.flatMapStateful(
     *         LongAccumulator::new,
     *         (counter, item) -> {
     *             counter.add(1);
     *             return counter.get() % 10 == 0
     *                     ? Traversers.traverseItems("punctuation", item)
     *                     : Traversers.singleton(item);
     *         }
     * );
     * }</pre>
     *
     * @param createFn  function that returns the state object
     * @param flatMapFn function that receives the state object and the input item and
     *                  outputs the result items. It may modify the state
     *                  object. It must not return null traverser, but can
     *                  return an {@linkplain Traversers#empty() empty traverser}.
     * @param <S>       type of the state object
     * @param <R>       type of the result
     */
    @Nonnull
    <S, R> GeneralStage<R> flatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    );

    /**
     * Attaches a rolling aggregation stage. This is a special case of
     * {@linkplain #mapStateful stateful mapping} that uses an {@link
     * AggregateOperation1 AggregateOperation}. It passes each input item to
     * the accumulator and outputs the current result of aggregation (as
     * returned by the {@link AggregateOperation1#exportFn() export} primitive).
     * <p>
     * Sample usage:
     * <pre>{@code
     * stage.rollingAggregate(AggregateOperations.summing())
     * }</pre>
     * For example, if your input is {@code {2, 7, 8, -5}}, the output will be
     * {@code {2, 9, 17, 12}}.
     * <p>
     * This stage is fault-tolerant and saves its state to the snapshot.
     * <p>
     * <strong>NOTE:</strong> since the output for each item depends on all
     * the previous items, this operation cannot be parallelized. Jet will
     * perform it on a single member, single-threaded. Jet also supports
     * {@link GeneralStageWithKey#rollingAggregate keyed rolling aggregation}
     * which it can parallelize by partitioning.
     *
     * @param aggrOp the aggregate operation to do the aggregation
     * @param <R> result type of the aggregate operation
     * @return the newly attached stage
     */
    @Nonnull
    default <A, R> GeneralStage<R> rollingAggregate(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        BiConsumer<? super A, ? super T> accumulateFn = aggrOp.accumulateFn();
        Function<? super A, ? extends R> exportFn = aggrOp.exportFn();
        return mapStateful(aggrOp.createFn(), (acc, item) -> {
            accumulateFn.accept(acc, item);
            return exportFn.apply(acc);
        });
    }

    /**
     * Attaches a mapping stage which applies the supplied function to each
     * input item independently and emits the function's result as the output
     * item. The mapping function receives another parameter, the service
     * object, which Jet will create using the supplied {@code serviceFactory}.
     * <p>
     * If the mapping result is {@code null}, it emits nothing. Therefore this
     * stage can be used to implement filtering semantics as well.
     * <p>
     * This sample takes a stream of stock items and sets the {@code detail}
     * field on them by looking up from a registry:
     * <pre>{@code
     * stage.mapUsingService(
     *     ServiceFactory.withCreateFn(jet -> new ItemDetailRegistry(jet)),
     *     (reg, item) -> item.setDetail(reg.fetchDetail(item))
     * )
     * }</pre>
     *
     * <h3>Interaction with fault-tolerant unbounded jobs</h3>
     *
     * If you use this stage in a fault-tolerant unbounded job, keep in mind
     * that any state the service object maintains doesn't participate in Jet's
     * fault tolerance protocol. If the state is local, it will be lost after a
     * job restart; if it is saved to some durable storage, the state of that
     * storage won't be rewound to the last checkpoint, so you'll perform
     * duplicate updates.
     *
     * @param serviceFactory the service factory
     * @param mapFn a stateless mapping function
     * @param <S> type of service object
     * @param <R> the result type of the mapping function
     * @return the newly attached stage
     */
    @Nonnull
    <S, R> GeneralStage<R> mapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    );

    /**
     * Asynchronous version of {@link #mapUsingService}: the {@code mapAsyncFn}
     * returns a {@code CompletableFuture<R>} instead of just {@code R}.
     * <p>
     * Uses default values for some extra parameters, so the maximum number
     * of concurrent async operations per processor will be limited to
     * {@value AbstractAsyncTransformUsingServiceP#DEFAULT_MAX_CONCURRENT_OPS} and
     * whether or not the order of input items should be preserved will be
     * {@value AbstractAsyncTransformUsingServiceP#DEFAULT_PRESERVE_ORDER}.
     * <p>
     * The function can return a null future or the future can return a null
     * result: in both cases it will act just like a filter.
     * <p>
     * The latency of the async call will add to the total latency of the
     * output.
     * <p>
     * This sample takes a stream of stock items and sets the {@code detail}
     * field on them by looking up from a registry:
     * <pre>{@code
     * stage.mapUsingServiceAsync(
     *     ServiceFactory.withCreateFn(jet -> new ItemDetailRegistry(jet)),
     *     (reg, item) -> reg.fetchDetailAsync(item)
     *                       .thenApply(detail -> item.setDetail(detail))
     * )
     * }</pre>
     *
     * <h3>Interaction with fault-tolerant unbounded jobs</h3>
     *
     * If you use this stage in a fault-tolerant unbounded job, keep in mind
     * that any state the service object maintains doesn't participate in Jet's
     * fault tolerance protocol. If the state is local, it will be lost after a
     * job restart; if it is saved to some durable storage, the state of that
     * storage won't be rewound to the last checkpoint, so you'll perform
     * duplicate updates.
     *
     * @param serviceFactory the service factory
     * @param mapAsyncFn a stateless mapping function. Can map to null (return
     *      a null future)
     * @param <S> type of service object
     * @param <R> the future result type of the mapping function
     * @return the newly attached stage
     */
    @Nonnull
    default <S, R> GeneralStage<R> mapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<R>> mapAsyncFn
    ) {
        return mapUsingServiceAsync(serviceFactory, DEFAULT_MAX_CONCURRENT_OPS, DEFAULT_PRESERVE_ORDER, mapAsyncFn);
    }

    /**
     * Asynchronous version of {@link #mapUsingService}: the {@code mapAsyncFn}
     * returns a {@code CompletableFuture<R>} instead of just {@code R}.
     * <p>
     * The function can return a null future or the future can return a null
     * result: in both cases it will act just like a filter.
     * <p>
     * The latency of the async call will add to the total latency of the
     * output.
     * <p>
     * This sample takes a stream of stock items and sets the {@code detail}
     * field on them by looking up from a registry:
     * <pre>{@code
     * stage.mapUsingServiceAsync(
     *     ServiceFactory.withCreateFn(jet -> new ItemDetailRegistry(jet)),
     *     (reg, item) -> reg.fetchDetailAsync(item)
     *                       .thenApply(detail -> item.setDetail(detail))
     * )
     * }</pre>
     *
     * <h3>Interaction with fault-tolerant unbounded jobs</h3>
     *
     * If you use this stage in a fault-tolerant unbounded job, keep in mind
     * that any state the service object maintains doesn't participate in Jet's
     * fault tolerance protocol. If the state is local, it will be lost after a
     * job restart; if it is saved to some durable storage, the state of that
     * storage won't be rewound to the last checkpoint, so you'll perform
     * duplicate updates.
     *
     * @param serviceFactory the service factory
     * @param maxConcurrentOps maximum number of concurrent async operations per processor
     * @param preserveOrder whether the ordering of the input items should be preserved
     * @param mapAsyncFn a stateless mapping function. Can map to null (return
     *      a null future)
     * @param <S> type of service object
     * @param <R> the future result type of the mapping function
     * @return the newly attached stage
     */
    @Nonnull
    <S, R> GeneralStage<R> mapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxConcurrentOps,
            boolean preserveOrder,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<R>> mapAsyncFn
    );

    /**
     * Batched version of {@link #mapUsingServiceAsync}: {@code mapAsyncFn} takes
     * a list of input items and returns a {@code CompletableFuture<List<R>>}.
     * The size of the input list is limited by the given {@code maxBatchSize}.
     * <p>
     * The number of in-flight batches being completed asynchronously is
     * limited to {@value ComputeStageImplBase#MAX_CONCURRENT_ASYNC_BATCHES}
     * and this mapping operation always preserves the order of input elements.
     * <p>
     * This transform can perform filtering by putting {@code null} elements into
     * the output list.
     * <p>
     * The latency of the async call will add to the total latency of the
     * output.
     * <p>
     * This sample takes a stream of stock items and sets the {@code detail}
     * field on them by performing batched lookups from a registry. The max
     * size of the items to lookup is specified as {@code 100}:
     * <pre>{@code
     * stage.mapUsingServiceAsyncBatched(
     *     ServiceFactory.withCreateFn(jet -> new ItemDetailRegistry(jet)),
     *     100,
     *     (reg, itemList) -> reg
     *             .fetchDetailsAsync(itemList)
     *             .thenApply(detailList -> {
     *                 for (int i = 0; i < itemList.size(); i++) {
     *                     itemList.get(i).setDetail(detailList.get(i))
     *                 }
     *             })
     * )
     * }</pre>
     *
     * <h3>Interaction with fault-tolerant unbounded jobs</h3>
     *
     * If you use this stage in a fault-tolerant unbounded job, keep in mind
     * that any state the service object maintains doesn't participate in Jet's
     * fault tolerance protocol. If the state is local, it will be lost after a
     * job restart; if it is saved to some durable storage, the state of that
     * storage won't be rewound to the last checkpoint, so you'll perform
     * duplicate updates.
     *
     * @param serviceFactory the service factory
     * @param maxBatchSize max size of the input list
     * @param mapAsyncFn a stateless mapping function
     * @param <S> type of service object
     * @param <R> the future result type of the mapping function
     * @return the newly attached stage
     * @since 4.0
     */
    @Nonnull
    <S, R> GeneralStage<R> mapUsingServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<List<R>>> mapAsyncFn
    );

    /**
     * Attaches a filtering stage which applies the provided predicate function
     * to each input item to decide whether to pass the item to the output or
     * to discard it. The predicate function receives another parameter, the
     * service object, which Jet will create using the supplied {@code
     * serviceFactory}.
     * <p>
     * This sample takes a stream of photos, uses an image classifier to reason
     * about their contents, and keeps only photos of cats:
     * <pre>{@code
     * photos.filterUsingService(
     *     ServiceFactory.withCreateFn(jet -> new ImageClassifier(jet)),
     *     (classifier, photo) -> classifier.classify(photo).equals("cat")
     * )
     * }</pre>
     *
     * <h3>Interaction with fault-tolerant unbounded jobs</h3>
     *
     * If you use this stage in a fault-tolerant unbounded job, keep in mind
     * that any state the service object maintains doesn't participate in Jet's
     * fault tolerance protocol. If the state is local, it will be lost after a
     * job restart; if it is saved to some durable storage, the state of that
     * storage won't be rewound to the last checkpoint, so you'll perform
     * duplicate updates.
     *
     * @param serviceFactory the service factory
     * @param filterFn a stateless filter predicate function
     * @param <S> type of service object
     * @return the newly attached stage
     */
    @Nonnull
    <S> GeneralStage<T> filterUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    );

    /**
     * Attaches a flat-mapping stage which applies the supplied function to
     * each input item independently and emits all items from the {@link
     * Traverser} it returns as the output items. The traverser must be
     * <em>null-terminated</em>. The mapping function receives another
     * parameter, the service object, which Jet will create using the supplied
     * {@code serviceFactory}.
     * <p>
     * This sample takes a stream of products and outputs an "exploded" stream
     * of all the parts that go into making them:
     * <pre>{@code
     * StreamStage<Part> parts = products.flatMapUsingService(
     *     ServiceFactory.withCreateFn(jet -> new PartRegistryCtx()),
     *     (registry, product) -> Traversers.traverseIterable(
     *                                registry.fetchParts(product))
     * );
     * }</pre>
     *
     * <h3>Interaction with fault-tolerant unbounded jobs</h3>
     *
     * If you use this stage in a fault-tolerant unbounded job, keep in mind
     * that any state the service object maintains doesn't participate in Jet's
     * fault tolerance protocol. If the state is local, it will be lost after a
     * job restart; if it is saved to some durable storage, the state of that
     * storage won't be rewound to the last checkpoint, so you'll perform
     * duplicate updates.
     *
     * @param serviceFactory the service factory
     * @param flatMapFn a stateless flatmapping function, whose result type is Jet's {@link
     *                  Traverser}. It must not return null traverser, but can return an
     *                  {@linkplain Traversers#empty() empty traverser}.
     * @param <S> type of service object
     * @param <R> the type of items in the result's traversers
     * @return the newly attached stage
     */
    @Nonnull
    <S, R> GeneralStage<R> flatMapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    );

    /**
     * Attaches a mapping stage where for each item a lookup in the
     * {@code ReplicatedMap} with the supplied name is performed and the
     * result of the lookup is merged with the item and emitted.
     * <p>
     * If the result of the mapping is {@code null}, it emits nothing.
     * Therefore this stage can be used to implement filtering semantics as
     * well.
     * <p>
     * The mapping logic is equivalent to:
     * <pre>{@code
     * K key = lookupKeyFn.apply(item);
     * V value = replicatedMap.get(key);
     * return mapFn.apply(item, value);
     * }</pre>
     *
     * This sample takes a stream of stock items and sets the {@code detail}
     * field on them by looking up from a registry:
     * <pre>{@code
     * items.mapUsingReplicatedMap(
     *     "enriching-map",
     *     item -> item.getDetailId(),
     *     (Item item, ItemDetail detail) -> item.setDetail(detail)
     * )
     * }</pre>
     *
     * @param mapName name of the {@code ReplicatedMap}
     * @param lookupKeyFn a function which returns the key to look up in the
     *          map. Must not return null
     * @param mapFn the mapping function
     * @param <K> type of the key in the {@code ReplicatedMap}
     * @param <V> type of the value in the {@code ReplicatedMap}
     * @param <R> type of the output item
     * @return the newly attached stage
     */
    @Nonnull
    default <K, V, R> GeneralStage<R> mapUsingReplicatedMap(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> lookupKeyFn,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        GeneralStage<R> res = mapUsingService(ServiceFactories.<K, V>replicatedMapService(mapName),
                (map, t) -> mapFn.apply(t, map.get(lookupKeyFn.apply(t))));
        return res.setName("mapUsingReplicatedMap");
    }

    /**
     * Attaches a mapping stage where for each item a lookup in the
     * supplied {@code ReplicatedMap} is performed and the result of the
     * lookup is merged with the item and emitted.
     * <p>
     * If the result of the mapping is {@code null}, it emits nothing.
     * Therefore this stage can be used to implement filtering semantics as well.
     * <p>
     * The mapping logic is equivalent to:
     * <pre>{@code
     * K key = lookupKeyFn.apply(item);
     * V value = replicatedMap.get(key);
     * return mapFn.apply(item, value);
     * }</pre>
     *
     * This sample takes a stream of stock items and sets the {@code detail}
     * field on them by looking up from a registry:
     * <pre>{@code
     * items.mapUsingReplicatedMap(
     *     enrichingMap,
     *     item -> item.getDetailId(),
     *     (item, detail) -> item.setDetail(detail)
     * )
     * }</pre>
     *
     * @param replicatedMap the {@code ReplicatedMap} to lookup from
     * @param lookupKeyFn a function which returns the key to look up in the
     *          map. Must not return null
     * @param mapFn the mapping function
     * @param <K> type of the key in the {@code ReplicatedMap}
     * @param <V> type of the value in the {@code ReplicatedMap}
     * @param <R> type of the output item
     * @return the newly attached stage
     */
    @Nonnull
    default <K, V, R> GeneralStage<R> mapUsingReplicatedMap(
            @Nonnull ReplicatedMap<K, V> replicatedMap,
            @Nonnull FunctionEx<? super T, ? extends K> lookupKeyFn,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return mapUsingReplicatedMap(replicatedMap.getName(), lookupKeyFn, mapFn);
    }

    /**
     * Attaches a mapping stage where for each item a lookup in the
     * {@code IMap} with the supplied name is performed and the
     * result of the lookup is merged with the item and emitted.
     * <p>
     * If the result of the mapping is {@code null}, it emits nothing.
     * Therefore this stage can be used to implement filtering semantics as well.
     * <p>
     * The mapping logic is equivalent to:
     *
     * <pre>{@code
     * K key = lookupKeyFn.apply(item);
     * V value = map.get(key);
     * return mapFn.apply(item, value);
     * }</pre>
     *
     * This sample takes a stream of stock items and sets the {@code detail}
     * field on them by looking up from a registry:
     * <pre>{@code
     * items.mapUsingIMap(
     *     "enriching-map",
     *     item -> item.getDetailId(),
     *     (Item item, ItemDetail detail) -> item.setDetail(detail)
     * )
     * }</pre>
     *
     * See also {@link GeneralStageWithKey#mapUsingIMap} for a partitioned version of
     * this operation.
     *
     * @param mapName name of the {@code IMap}
     * @param lookupKeyFn a function which returns the key to look up in the
     *          map. Must not return null
     * @param mapFn the mapping function
     * @param <K> type of the key in the {@code IMap}
     * @param <V> type of the value in the {@code IMap}
     * @param <R> type of the output item
     * @return the newly attached stage
     */
    @Nonnull
    default <K, V, R> GeneralStage<R> mapUsingIMap(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> lookupKeyFn,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        GeneralStage<R> res = mapUsingServiceAsync(
                ServiceFactories.<K, V>iMapService(mapName),
                DEFAULT_MAX_CONCURRENT_OPS,
                DEFAULT_PRESERVE_ORDER,
                (map, t) -> map.getAsync(lookupKeyFn.apply(t)).toCompletableFuture().thenApply(e -> mapFn.apply(t, e))
        );
        return res.setName("mapUsingIMap");
    }

    /**
     * Attaches a mapping stage where for each item a lookup in the
     * supplied {@code IMap} is performed and the result of the
     * lookup is merged with the item and emitted.
     * <p>
     * If the result of the mapping is {@code null}, it emits nothing.
     * Therefore this stage can be used to implement filtering semantics as well.
     * <p>
     * The mapping logic is equivalent to:
     *
     * <pre>{@code
     * K key = lookupKeyFn.apply(item);
     * V value = map.get(key);
     * return mapFn.apply(item, value);
     * }</pre>
     *
     * This sample takes a stream of stock items and sets the {@code detail}
     * field on them by looking up from a registry:
     * <pre>{@code
     * items.mapUsingIMap(
     *     enrichingMap,
     *     item -> item.getDetailId(),
     *     (item, detail) -> item.setDetail(detail)
     * )
     * }</pre>
     *
     * See also {@link GeneralStageWithKey#mapUsingIMap} for a partitioned version of
     * this operation.
     *
     * @param iMap the {@code IMap} to lookup from
     * @param lookupKeyFn a function which returns the key to look up in the
     *          map. Must not return null
     * @param mapFn the mapping function
     * @param <K> type of the key in the {@code IMap}
     * @param <V> type of the value in the {@code IMap}
     * @param <R> type of the output item
     * @return the newly attached stage
     */
    @Nonnull
    default <K, V, R> GeneralStage<R> mapUsingIMap(
            @Nonnull IMap<K, V> iMap,
            @Nonnull FunctionEx<? super T, ? extends K> lookupKeyFn,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return mapUsingIMap(iMap.getName(), lookupKeyFn, mapFn);
    }

    /**
     * Attaches to both this and the supplied stage a hash-joining stage and
     * returns it. This stage plays the role of the <em>primary stage</em> in
     * the hash-join. Please refer to the {@link com.hazelcast.jet.pipeline
     * package javadoc} for a detailed description of the hash-join transform.
     * <p>
     * This sample joins a stream of users to a stream of countries and outputs
     * a stream of users with the {@code country} field set:
     * <pre>{@code
     * // Types of the input stages:
     * BatchStage<User> users;
     * BatchStage<Map.Entry<Long, Country>> idAndCountry;
     *
     * users.hashJoin(
     *     idAndCountry,
     *     JoinClause.joinMapEntries(User::getCountryId),
     *     (user, country) -> user.setCountry(country)
     * )
     * }</pre>
     *
     * @param stage1        the stage to hash-join with this one
     * @param joinClause1   specifies how to join the two streams
     * @param mapToOutputFn function to map the joined items to the output value
     * @param <K>           the type of the join key
     * @param <T1_IN>       the type of {@code stage1} items
     * @param <T1>          the result type of projection on {@code stage1} items
     * @param <R>           the resulting output type
     * @return the newly attached stage
     */
    @Nonnull
    <K, T1_IN, T1, R> GeneralStage<R> hashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BiFunctionEx<T, T1, R> mapToOutputFn
    );

    /**
     * Attaches to this and the two supplied stages a hash-joining stage and
     * returns it. This stage plays the role of the <em>primary stage</em> in
     * the hash-join. Please refer to the {@link com.hazelcast.jet.pipeline
     * package javadoc} for a detailed description of the hash-join transform.
     * <p>
     * This sample joins a stream of users to streams of countries and
     * companies, and outputs a stream of users with the {@code country} and
     * {@code company} fields set:
     * <pre>{@code
     * // Types of the input stages:
     * BatchStage<User> users;
     * BatchStage<Map.Entry<Long, Country>> idAndCountry;
     * BatchStage<Map.Entry<Long, Company>> idAndCompany;
     *
     * users.hashJoin(
     *     idAndCountry, JoinClause.joinMapEntries(User::getCountryId),
     *     idAndCompany, JoinClause.joinMapEntries(User::getCompanyId),
     *     (user, country, company) -> user.setCountry(country).setCompany(company)
     * )
     * }</pre>
     *
     * @param stage1        the first stage to join
     * @param joinClause1   specifies how to join with {@code stage1}
     * @param stage2        the second stage to join
     * @param joinClause2   specifies how to join with {@code stage2}
     * @param mapToOutputFn function to map the joined items to the output value
     * @param <K1>          the type of key for {@code stage1}
     * @param <T1_IN>       the type of {@code stage1} items
     * @param <T1>          the result type of projection of {@code stage1} items
     * @param <K2>          the type of key for {@code stage2}
     * @param <T2_IN>       the type of {@code stage2} items
     * @param <T2>          the result type of projection of {@code stage2} items
     * @param <R>           the resulting output type
     * @return the newly attached stage
     */
    @Nonnull
    <K1, K2, T1_IN, T2_IN, T1, T2, R> GeneralStage<R> hashJoin2(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull TriFunction<T, T1, T2, R> mapToOutputFn
    );

    /**
     * Returns a fluent API builder object to construct a hash join operation
     * with any number of contributing stages. It is mainly intended for
     * hash-joins with three or more enriching stages. For one or two stages
     * prefer the direct {@code stage.hashJoinN(...)} calls because they offer
     * more static type safety.
     * <p>
     * This sample joins a stream of users to streams of countries and
     * companies, and outputs a stream of users with the {@code country} and
     * {@code company} fields set:
     * <pre>{@code
     * // Types of the input stages:
     * StreamStage<User> users;
     * BatchStage<Map.Entry<Long, Country>> idAndCountry;
     * BatchStage<Map.Entry<Long, Company>> idAndCompany;
     *
     * StreamHashJoinBuilder<User> builder = users.hashJoinBuilder();
     * Tag<Country> tCountry = builder.add(idAndCountry,
     *         JoinClause.joinMapEntries(User::getCountryId));
     * Tag<Company> tCompany = builder.add(idAndCompany,
     *         JoinClause.joinMapEntries(User::getCompanyId));
     * StreamStage<User> joined = builder.build((user, itemsByTag) ->
     *         user.setCountry(itemsByTag.get(tCountry)).setCompany(itemsByTag.get(tCompany)));
     * }</pre>
     *
     * @return the newly attached stage
     */
    @Nonnull
    GeneralHashJoinBuilder<T> hashJoinBuilder();

    /**
     * Specifies the function that will extract a key from the items in the
     * associated pipeline stage. This enables the operations that need the
     * key, such as grouped aggregation.
     * <p>
     * Sample usage:
     * <pre>{@code
     * users.groupingKey(User::getId)
     * }</pre>
     * <p>
     * <b>Note:</b> make sure the extracted key is not-null, it would fail the
     * job otherwise. Also make sure that it implements {@code equals()} and
     * {@code hashCode()}.
     *
     * @param keyFn function that extracts the grouping key
     * @param <K> type of the key
     * @return the newly attached stage
     */
    @Nonnull
    <K> GeneralStageWithKey<T, K> groupingKey(@Nonnull FunctionEx<? super T, ? extends K> keyFn);

    /**
     * Adds a timestamp to each item in the stream using the supplied function
     * and specifies the allowed amount of disorder between them. As the stream
     * moves on, the timestamps must increase, but you can tell Jet to accept
     * some items that "come in late", i.e., have a lower timestamp than the
     * items before them. The {@code allowedLag} parameter controls by how much
     * the timestamp can be lower than the highest one observed so far. If
     * it is even lower, Jet will drop the item as being "too late".
     * <p>
     * For example, if the sequence of the timestamps is {@code [1,4,3,2]} and
     * you configured the allowed lag as {@code 1}, Jet will let through the
     * event with timestamp {@code 3}, but it will drop the last one (timestamp
     * {@code 2}).
     * <p>
     * The amount of lag you configure strongly influences the latency of Jet's
     * output. Jet cannot finalize the window until it knows it has observed all
     * the events belonging to it, and the more lag it must tolerate, the longer
     * will it have to wait for possible latecomers. On the other hand, if you
     * don't allow enough lag, you face the risk of failing to account for the
     * data that came in after the results were already emitted.
     * <p>
     * Sample usage:
     * <pre>{@code
     * events.addTimestamps(Event::getTimestamp, 1000)
     * }</pre>
     * <p>
     * <b>Note:</b> This method adds the timestamps after the source emitted
     * them. When timestamps are added at this moment, source partitions won't
     * be coalesced properly and will be treated as a single stream. The
     * allowed lag will need to cover for the additional disorder introduced by
     * merging the streams. The streams are merged in an unpredictable order
     * and it can happen, for example, that after the job was suspended for a
     * long time, there can be a very recent event in partition1 and a very old
     * event partition2. If partition1 happens to be merged first, the recent
     * event could render the old one late, if the allowed lag is not large
     * enough.<br>
     * To add timestamps in source, use {@link
     * StreamSourceStage#withTimestamps(ToLongFunctionEx, long)
     * withTimestamps()}.
     * <p>
     * <b>Warning:</b> make sure the property you access in {@code timestampFn}
     * isn't null, it would fail the job. Also that there are no nonsensical
     * values such as -1, MIN_VALUE, 2100-01-01 etc - we'll treat those as real
     * timestamps and they can cause unspecified behaviour.
     *
     * @param timestampFn a function that returns the timestamp for each item,
     *                    typically in milliseconds
     * @param allowedLag the allowed lag behind the top observed timestamp.
     *                   Time unit is the same as the unit used by {@code
     *                   timestampFn}
     * @return the newly attached stage
     * @throws IllegalArgumentException if this stage already has timestamps
     */
    @Nonnull
    StreamStage<T> addTimestamps(@Nonnull ToLongFunctionEx<? super T> timestampFn, long allowedLag);

    /**
     * Attaches a sink stage, one that accepts data but doesn't emit any. The
     * supplied argument specifies what to do with the received data (typically
     * push it to some outside resource).
     * <p>
     * You cannot reuse the sink in other {@code writeTo} calls. If you want to
     * write multiple stages to the same sink, use {@link Pipeline#writeTo}.
     * This will be more efficient than creating a new sink each time.
     *
     * @return the newly attached sink stage
     */
    @Nonnull
    SinkStage writeTo(@Nonnull Sink<? super T> sink);

    /**
     * Attaches a peeking stage which logs this stage's output and passes it
     * through without transformation. For each item the stage emits, it:
     * <ol><li>
     *     uses the {@code shouldLogFn} predicate to see whether to log the item
     * </li><li>
     *     if yes, uses then uses {@code toStringFn} to get the item's string
     *     representation
     * </li><li>
     *     logs the string at the INFO level to the log category {@code
     *     com.hazelcast.jet.impl.processor.PeekWrappedP.<vertexName>#<processorIndex>}
     * </li></ol>
     * The stage logs each item on whichever cluster member it happens to
     * receive it. Its primary purpose is for development use, when running Jet
     * on a local machine.
     * <p>
     * Sample usage:
     * <pre>{@code
     * users.peek(
     *     user -> user.getName().size() > 100,
     *     User::getName
     * )
     * }</pre>
     *
     * @param shouldLogFn a function to filter the logged items. You can use {@link
     *                    PredicateEx#alwaysTrue()
     *                    alwaysTrue()} as a pass-through filter when you don't need any
     *                    filtering.
     * @param toStringFn  a function that returns a string representation of the item
     * @return the newly attached stage
     * @see #peek(FunctionEx)
     * @see #peek()
     */
    @Nonnull
    GeneralStage<T> peek(
            @Nonnull PredicateEx<? super T> shouldLogFn,
            @Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn
    );

    /**
     * Adds a peeking layer to this compute stage which logs its output. For
     * each item the stage emits, it:
     * <ol><li>
     *     uses {@code toStringFn} to get a string representation of the item
     * </li><li>
     *     logs the string at the INFO level to the log category {@code
     *     com.hazelcast.jet.impl.processor.PeekWrappedP.<vertexName>#<processorIndex>}
     * </li></ol>
     * The stage logs each item on whichever cluster member it happens to
     * receive it. Its primary purpose is for development use, when running Jet
     * on a local machine.
     * <p>
     * Sample usage:
     * <pre>{@code
     * users.peek(User::getName)
     * }</pre>
     *
     * @param toStringFn  a function that returns a string representation of the item
     * @return the newly attached stage
     * @see #peek(PredicateEx, FunctionEx)
     * @see #peek()
     */
    @Nonnull
    default GeneralStage<T> peek(@Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn) {
        return peek(alwaysTrue(), toStringFn);
    }

    /**
     * Adds a peeking layer to this compute stage which logs its output. For
     * each item the stage emits, it logs the result of its {@code toString()}
     * method at the INFO level to the log category {@code
     * com.hazelcast.jet.impl.processor.PeekWrappedP.<vertexName>#<processorIndex>}.
     * The stage logs each item on whichever cluster member it happens to
     * receive it. Its primary purpose is for development use, when running Jet
     * on a local machine.
     *
     * @return the newly attached stage
     * @see #peek(PredicateEx, FunctionEx)
     * @see #peek(FunctionEx)
     */
    @Nonnull
    default GeneralStage<T> peek() {
        return peek(alwaysTrue(), Object::toString);
    }

    /**
     * Attaches a stage with a custom transform based on the provided supplier
     * of Core API {@link Processor}s.
     * <p>
     * Note that the type parameter of the returned stage is inferred from the
     * call site and not propagated from the processor that will produce the
     * result, so there is no actual type safety provided.
     *
     * @param stageName    a human-readable name for the custom stage
     * @param procSupplier the supplier of processors
     * @param <R>          the type of the output items
     * @return the newly attached stage
     */
    @Nonnull
    <R> GeneralStage<R> customTransform(
            @Nonnull String stageName, @Nonnull SupplierEx<Processor> procSupplier);

    /**
     * Attaches a stage with a custom transform based on the provided supplier
     * of Core API {@link Processor}s.
     * <p>
     * Note that the type parameter of the returned stage is inferred from the
     * call site and not propagated from the processor that will produce the
     * result, so there is no actual type safety provided.
     *
     * @param stageName    a human-readable name for the custom stage
     * @param procSupplier the supplier of processors
     * @param <R>          the type of the output items
     * @return the newly attached stage
     */
    @Nonnull
    <R> GeneralStage<R> customTransform(
            @Nonnull String stageName, @Nonnull ProcessorSupplier procSupplier);

    /**
     * Attaches a stage with a custom transform based on the provided supplier
     * of Core API {@link Processor}s.
     * <p>
     * Note that the type parameter of the returned stage is inferred from the
     * call site and not propagated from the processor that will produce the
     * result, so there is no actual type safety provided.
     *
     * @param stageName a human-readable name for the custom stage
     * @param procSupplier the supplier of processors
     * @param <R> the type of the output items
     * @return the newly attached stage
     */
    @Nonnull
    <R> GeneralStage<R> customTransform(
            @Nonnull String stageName, @Nonnull ProcessorMetaSupplier procSupplier);

    @Nonnull @Override
    GeneralStage<T> setLocalParallelism(int localParallelism);

    @Nonnull @Override
    GeneralStage<T> setName(@Nonnull String name);
}
