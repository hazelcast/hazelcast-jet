/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.WatermarkPolicy;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.function.DistributedTriFunction;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.function.DistributedFunctions.alwaysTrue;

/**
 * Represents a pipeline in a distributed computation {@link Pipeline
 * pipeline}. It accepts input from its upstream stages (if any) and passes
 * its output to its downstream stages.
 *
 * @param <T> the type of items coming out of this pipeline
 */
public interface GeneralStage<T> extends Stage {

    /**
     * Attaches to this pipeline a mapping pipeline, one which applies the supplied
     * function to each input item independently and emits the function's
     * result as the output item. Returns the newly attached pipeline.
     *
     * @param mapFn the mapping function
     * @param <R> the result type of the mapping function
     */
    @Nonnull
    <R> GeneralStage<R> map(@Nonnull DistributedFunction<? super T, ? extends R> mapFn);

    /**
     * Attaches to this pipeline a filtering pipeline, one which applies the provided
     * predicate function to each input item to decide whether to pass the item
     * to the output or to discard it. Returns the newly attached pipeline.
     *
     * @param filterFn the filter predicate function
     */
    @Nonnull
    GeneralStage<T> filter(@Nonnull DistributedPredicate<T> filterFn);

    /**
     * Attaches to this pipeline a flat-mapping pipeline, one which applies the
     * supplied function to each input item independently and emits all items
     * from the {@link Traverser} it returns as the output items. Returns the
     * newly attached pipeline.
     *
     * @param flatMapFn the flatmapping function, whose result type is Jet's {@link Traverser}
     * @param <R> the type of items in the result's traversers
     */
    @Nonnull
    <R> GeneralStage<R> flatMap(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    );

    /**
     * Attaches to both this and the supplied pipeline a hash-joining pipeline and
     * returns it. This pipeline plays the role of the <em>primary pipeline</em> in
     * the hash-join. Please refer to the {@link com.hazelcast.jet
     * package Javadoc} for a detailed description of the hash-join transform.
     *
     * @param stage1     the pipeline to hash-join with this one
     * @param joinClause1 specifies how to join the two streams
     * @param mapToOutputFn function to map the joined items to the output value
     * @param <K>        the type of the join key
     * @param <T1_IN>    the type of {@code stage1} items
     * @param <T1>       the result type of projection on {@code stage1} items
     * @param <R>        the resulting output type
     */
    @Nonnull
    <K, T1_IN, T1, R> GeneralStage<R> hashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull DistributedBiFunction<T, T1, R> mapToOutputFn
    );

    /**
     * Attaches to this and the two supplied stages a hash-joining pipeline and
     * returns it. This pipeline plays the role of the <em>primary pipeline</em> in
     * the hash-join. Please refer to the {@link com.hazelcast.jet package
     * Javadoc} for a detailed description of the hash-join transform.
     *
     * @param stage1      the first pipeline to join
     * @param joinClause1 specifies how to join with {@code stage1}
     * @param stage2      the second pipeline to join
     * @param joinClause2 specifices how to join with {@code stage2}
     * @param mapToOutputFn function to map the joined items to the output value
     * @param <K1>        the type of key for {@code stage1}
     * @param <T1_IN>     the type of {@code stage1} items
     * @param <T1>        the result type of projection of {@code stage1} items
     * @param <K2>        the type of key for {@code stage2}
     * @param <T2_IN>     the type of {@code stage2} items
     * @param <T2>        the result type of projection of {@code stage2} items
     * @param <R>         the resulting output type
     */
    @Nonnull
    <K1, T1_IN, T1, K2, T2_IN, T2, R> GeneralStage<R> hashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull DistributedTriFunction<T, T1, T2, R> mapToOutputFn
    );

    /**
     * Returns a fluent API builder object to construct a hash join operation
     * with any number of contributing stages. This object is mainly intended
     * to build a hash-join of the primary pipeline with three or more
     * contributing stages. For one or two stages the direct
     * {@code pipeline.hashJoin(...)} calls should be preferred because they offer
     * more static type safety.
     */
    @Nonnull
    GeneralHashJoinBuilder<T> hashJoinBuilder();

    @Nonnull
    <K> GeneralStageWithGrouping<T, K> groupingKey(@Nonnull DistributedFunction<? super T, ? extends K> keyFn);

    @Nonnull
    StreamStage<T> timestamp(
            @Nonnull DistributedToLongFunction<? super T> timestampFn,
            @Nonnull WatermarkPolicy wmPolicy
    );

    /**
     * Attaches to this pipeline a sink pipeline, one that accepts data but doesn't
     * emit any. The supplied argument specifies what to do with the received
     * data (typically push it to some outside resource).
     */
    @Nonnull
    SinkStage drainTo(@Nonnull Sink<? super T> sink);

    /**
     * Adds a peeking layer to this compute pipeline which logs its output. For
     * each item the pipeline emits, it:
     * <ol><li>
     *     uses the {@code shouldLogFn} predicate to see whether to log the item
     * </li><li>
     *     if the item passed, uses {@code toStringFn} to get a string
     *     representation of the item
     * </li><li>
     *     logs the string at the INFO level to the log category {@code
     *     com.hazelcast.jet.impl.processor.PeekWrappedP.<vertexName>#<processorIndex>}
     * </li></ol>
     * The pipeline logs each item on whichever cluster member it happens to
     * receive it. Its primary purpose is for development use, when running Jet
     * on a local machine.
     *
     * @param shouldLogFn a function to filter the logged items. You can use {@link
     *                    com.hazelcast.jet.function.DistributedFunctions#alwaysTrue()
     *                    alwaysTrue()} as a pass-through filter when you don't need any
     *                    filtering.
     * @param toStringFn  a function that returns a string representation of the item
     * @see #peek(DistributedFunction)
     * @see #peek()
     */
    @Nonnull
    GeneralStage<T> peek(
            @Nonnull DistributedPredicate<? super T> shouldLogFn,
            @Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn
    );

    /**
     * Adds a peeking layer to this compute pipeline which logs its output. For
     * each item the pipeline emits, it:
     * <ol><li>
     *     uses {@code toStringFn} to get a string representation of the item
     * </li><li>
     *     logs the string at the INFO level to the log category {@code
     *     com.hazelcast.jet.impl.processor.PeekWrappedP.<vertexName>#<processorIndex>}
     * </li></ol>
     * The pipeline logs each item on whichever cluster member it happens to
     * receive it. Its primary purpose is for development use, when running Jet
     * on a local machine.
     *
     * @param toStringFn  a function that returns a string representation of the item
     * @see #peek(DistributedPredicate, DistributedFunction)
     * @see #peek()
     */
    @Nonnull
    default GeneralStage<T> peek(@Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn) {
        return peek(alwaysTrue(), toStringFn);
    }

    /**
     * Adds a peeking layer to this compute pipeline which logs its output. For
     * each item the pipeline emits, it logs the result of its {@code toString()}
     * method at the INFO level to the log category {@code
     * com.hazelcast.jet.impl.processor.PeekWrappedP.<vertexName>#<processorIndex>}.
     * The pipeline logs each item on whichever cluster member it happens to
     * receive it. Its primary purpose is for development use, when running Jet
     * on a local machine.
     *
     * @see #peek(DistributedPredicate, DistributedFunction)
     * @see #peek(DistributedFunction)
     */
    @Nonnull
    default GeneralStage<T> peek() {
        return peek(alwaysTrue(), Object::toString);
    }

    /**
     * Attaches to this pipeline a pipeline with a custom transform based on the
     * provided supplier of Core API {@link Processor}s. To be compatible with
     * the rest of the pipeline, the processor must expect a single inbound
     * edge and arbitrarily many outbound edges, and it must push the same data
     * to all outbound edges.
     * <p>
     * Note that the returned pipeline's type parameter is inferred from the call
     * site and not propagated from the processor that will produce the result,
     * so there is no actual type safety provided.
     *
     * @param stageName a human-readable name for the custom pipeline
     * @param procSupplier the supplier of processors
     * @param <R> the type of the output items
     */
    @Nonnull
    <R> GeneralStage<R> customTransform(
            @Nonnull String stageName, @Nonnull DistributedSupplier<Processor> procSupplier);
}
