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

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BiPredicateEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.impl.processor.ProcessorWrapper;
import com.hazelcast.jet.impl.util.WrappingProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.JoinClause;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.JetEvent.jetEvent;

public class JetEventFunctionAdapter {
    public static final JetEventFunctionAdapter INSTANCE = new JetEventFunctionAdapter();

    @Nonnull
    <T> ToLongFunctionEx<? super JetEvent<T, ?>> adaptTimestampFn(ToLongFunctionEx<? super T> timestampFn) {
        return e -> {
            // TODO [viliam] could we detect this when building the pipeline, not at runtime?
            assert e.timestamp() == JetEvent.NO_TIMESTAMP : "addTimestamp used to override an already existing timestamp";
            return timestampFn.applyAsLong(e.payload());
        };
    }

    @SuppressWarnings("unused")
    @Nonnull
    public <T, K> FunctionEx<? super JetEvent<T, ?>, ? extends K> adaptKeyFn(
            @Nonnull FunctionEx<? super T, ? extends K> keyFn
    ) {
        return e -> keyFn.apply(e.payload());
    }

    @Nonnull
    <T> ToLongFunctionEx<? super JetEvent<T, ?>> adaptTimestampFn() {
        return JetEvent::timestamp;
    }

    @Nonnull
    <T, R> FunctionEx<? super JetEvent<T, ?>, ?> adaptMapFn(
            @Nonnull FunctionEx<? super T, ? extends R> mapFn
    ) {
        return e -> jetEvent(mapFn.apply(e.payload()), e.key(), e.timestamp());
    }

    @Nonnull
    <T> PredicateEx<? super JetEvent<T, ?>> adaptFilterFn(@Nonnull PredicateEx<? super T> filterFn) {
        return e -> filterFn.test(e.payload());
    }

    @Nonnull
    <T, R> FunctionEx<? super JetEvent<T, ?>, ? extends Traverser<JetEvent<R, ?>>> adaptFlatMapFn(
            @Nonnull FunctionEx<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return e -> flatMapFn.apply(e.payload()).map(r -> jetEvent(r, e.key(), e.timestamp()));
    }

    @Nonnull
    <S, K, T, R> TriFunction<? super S, ? super K, ? super JetEvent<T, K>, ? extends R> adaptStatefulMapFn(
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn
    ) {
        return (state, key, e) -> mapFn.apply(state, key, e.payload());
    }

    @Nonnull
    <S, K, R> TriFunction<? super S, ? super K, ? super Long, ? extends JetEvent<R, K>> adaptOnEvictFn(
            @Nonnull TriFunction<? super S, ? super K, ? super Long, ? extends R> onEvictFn
    ) {
        return (s, k, wm) -> jetEvent(onEvictFn.apply(s, k, wm), k, wm);
    }

    @Nonnull
    <S, K, T, R> TriFunction<? super S, ? super K, ? super JetEvent<T, K>, ? extends Traverser<JetEvent<R, K>>>
    adaptStatefulFlatMapFn(
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return (state, key, e) -> flatMapFn.apply(state, key, e.payload()).map(r -> jetEvent(r, key, e.timestamp()));
    }

    @Nonnull
    <S, K, R> TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<JetEvent<R, K>>> adaptOnEvictFlatMapFn(
            @Nonnull TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn
    ) {
        return (s, k, wm) -> onEvictFn.apply(s, k, wm).map(r -> jetEvent(r, k, wm));
    }

    @Nonnull
    <S, T, R> BiFunctionEx<? super S, ? super JetEvent<T, ?>, ? extends JetEvent<R, ?>> adaptMapUsingServiceFn(
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        return (s, e) -> jetEvent(mapFn.apply(s, e.payload()), e.key(), e.timestamp());
    }

    @Nonnull
    <S, T> BiPredicateEx<? super S, ? super JetEvent<T, ?>> adaptFilterUsingServiceFn(
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        return (s, e) -> filterFn.test(s, e.payload());
    }

    @Nonnull
    <S, T, R> BiFunctionEx<? super S, ? super JetEvent<T, ?>, ? extends Traverser<JetEvent<R, ?>>>
    adaptFlatMapUsingServiceFn(
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return (s, e) -> flatMapFn.apply(s, e.payload()).map(r -> jetEvent(r, e.key(), e.timestamp()));
    }

    @Nonnull
    <S, T, R> BiFunctionEx<? super S, ?, ? extends CompletableFuture<Traverser<?>>>
    adaptFlatMapUsingServiceAsyncFn(
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        return (S s, JetEvent<T, ?> e) ->
                flatMapAsyncFn.apply(s, e.payload())
                              .thenApply(trav -> trav.map(re -> jetEvent(re, e.key(), e.timestamp())));
    }

    @Nonnull
    <T, STR extends CharSequence> FunctionEx<? super JetEvent<T, ?>, ? extends STR> adaptToStringFn(
            @Nonnull FunctionEx<? super T, ? extends STR> toStringFn
    ) {
        return e -> toStringFn.apply(e.payload());
    }

    @Nonnull
    public <K, T0, T1, T1_OUT> JoinClause<? extends K, ? super JetEvent<T0, ?>, ? super T1, ? extends T1_OUT>
    adaptJoinClause(
            @Nonnull JoinClause<? extends K, ? super T0, ? super T1, ? extends T1_OUT> joinClause
    ) {
        return JoinClause.<K, JetEvent<T0, ?>, T1>onKeys(adaptKeyFn(joinClause.leftKeyFn()), joinClause.rightKeyFn())
                .projecting(joinClause.rightProjectFn());
    }

    @Nonnull
    public <T, T1, R> BiFunctionEx<? super JetEvent<T, ?>, ? super T1, ?> adaptHashJoinOutputFn(
            @Nonnull BiFunctionEx<? super T, ? super T1, ? extends R> mapToOutputFn
    ) {
        return (e, t1) -> jetEvent(mapToOutputFn.apply(e.payload(), t1), e.key(), e.timestamp());
    }

    @Nonnull
    <T, T1, T2, R> TriFunction<? super JetEvent<T, ?>, ? super T1, ? super T2, ?> adaptHashJoinOutputFn(
            @Nonnull TriFunction<? super T, ? super T1, ? super T2, ? extends R> mapToOutputFn
    ) {
        return (e, t1, t2) -> jetEvent(mapToOutputFn.apply(e.payload(), t1, t2), e.key(), e.timestamp());
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <A, R> AggregateOperation<A, ? extends R> adaptAggregateOperation(
            @Nonnull AggregateOperation<A, ? extends R> aggrOp
    ) {
        if (aggrOp instanceof AggregateOperation1) {
            return adaptAggregateOperation1((AggregateOperation1) aggrOp);
        } else if (aggrOp instanceof AggregateOperation2) {
            return adaptAggregateOperation2((AggregateOperation2) aggrOp);
        } else if (aggrOp instanceof AggregateOperation3) {
            return adaptAggregateOperation3((AggregateOperation3) aggrOp);
        } else {
            BiConsumerEx[] adaptedAccFns = new BiConsumerEx[aggrOp.arity()];
            Arrays.setAll(adaptedAccFns, i -> adaptAccumulateFn((BiConsumerEx) aggrOp.accumulateFn(i)));
            return aggrOp.withAccumulateFns(adaptedAccFns);
        }
    }

    @Nonnull
    <T, A, R> AggregateOperation1<? super JetEvent<T, ?>, A, ? extends R> adaptAggregateOperation1(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        return aggrOp.withAccumulateFn(adaptAccumulateFn(aggrOp.accumulateFn()));
    }

    @Nonnull
    static <T0, T1, A, R> AggregateOperation2<? super JetEvent<T0, ?>, ? super JetEvent<T1, ?>, A, ? extends R>
    adaptAggregateOperation2(@Nonnull AggregateOperation2<? super T0, ? super T1, A, ? extends R> aggrOp) {
        return aggrOp
                .<JetEvent<T0, ?>>withAccumulateFn0(adaptAccumulateFn(aggrOp.accumulateFn0()))
                .withAccumulateFn1(adaptAccumulateFn(aggrOp.accumulateFn1()));
    }

    @Nonnull
    static <T0, T1, T2, A, R>
    AggregateOperation3<? super JetEvent<T0, ?>, ? super JetEvent<T1, ?>, ? super JetEvent<T2, ?>, A, ? extends R>
    adaptAggregateOperation3(@Nonnull AggregateOperation3<? super T0, ? super T1, ? super T2, A, ? extends R> aggrOp) {
        return aggrOp
                .<JetEvent<T0, ?>>withAccumulateFn0(adaptAccumulateFn(aggrOp.accumulateFn0()))
                .<JetEvent<T1, ?>>withAccumulateFn1(adaptAccumulateFn(aggrOp.accumulateFn1()))
                .withAccumulateFn2(adaptAccumulateFn(aggrOp.accumulateFn2()));
    }

    @Nonnull
    private static <A, T> BiConsumerEx<? super A, ? super JetEvent<T, ?>> adaptAccumulateFn(
            @Nonnull BiConsumerEx<? super A, ? super T> accumulateFn
    ) {
        return (acc, t) -> accumulateFn.accept(acc, t.payload());
    }

    @Nonnull
    public static ProcessorMetaSupplier inputAdaptingMetaSupplier(ProcessorMetaSupplier metaSup) {
        return new WrappingProcessorMetaSupplier(metaSup, p -> new InputAdaptingProcessor(p));
    }

    private static final class InputAdaptingProcessor extends ProcessorWrapper {
        private final AdaptingInbox adaptingInbox = new AdaptingInbox();

        InputAdaptingProcessor(Processor wrapped) {
            super(wrapped);
        }

        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            adaptingInbox.setWrappedInbox(inbox);
            super.process(ordinal, adaptingInbox);
        }
    }

    private static final class AdaptingInbox implements Inbox {
        private Inbox wrapped;

        void setWrappedInbox(@Nonnull Inbox wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public boolean isEmpty() {
            return wrapped.isEmpty();
        }

        @Override
        public Object peek() {
            return unwrapPayload(wrapped.peek());
        }

        @Override
        public Object poll() {
            return unwrapPayload(wrapped.poll());
        }

        @Override
        public void remove() {
            wrapped.remove();
        }

        @Override
        public int size() {
            return wrapped.size();
        }

        private static Object unwrapPayload(Object jetEvent) {
            return jetEvent != null ? ((JetEvent) jetEvent).payload() : null;
        }
    }

    /**
     * Returns a processor supplier wrapper that passes to the given supplier
     * an outbox that will wrap all output items in {@code jetEvent(item,
     * globalProcessorIndex, NO_TIMESTAMP)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier outputAdaptingMetaSupplier(ProcessorMetaSupplier metaSup) {
        return new WrappingProcessorMetaSupplier(metaSup, OutputAdaptingProcessor::new);
    }

    private static final class OutputAdaptingProcessor extends ProcessorWrapper {
        OutputAdaptingProcessor(Processor wrapped) {
            super(wrapped);
        }

        @Override
        public void initWrapper(@Nonnull Outbox outbox, @Nonnull Context context) throws Exception {
            super.init(new AdaptingOutbox(outbox, context.globalProcessorIndex()), context);
        }
    }

    private static final class AdaptingOutbox implements Outbox {
        private final Outbox wrapped;
        private final Integer key;

        AdaptingOutbox(Outbox wrapped, Integer key) {
            this.wrapped = wrapped;
            this.key = key;
        }

        @Override
        public int bucketCount() {
            return wrapped.bucketCount();
        }

        @Override
        public boolean offer(int ordinal, @Nonnull Object item) {
            return wrapped.offer(ordinal, wrap(item));
        }

        @Override
        public boolean offer(@Nonnull int[] ordinals, @Nonnull Object item) {
            return wrapped.offer(ordinals, wrap(item));
        }

        @Override
        public boolean offerToSnapshot(@Nonnull Object key, @Nonnull Object value) {
            return wrapped.offerToSnapshot(key, value);
        }

        @Override
        public boolean offer(@Nonnull Object item) {
            return wrapped.offer(wrap(item));
        }

        @Override
        public boolean hasUnfinishedItem() {
            return wrapped.hasUnfinishedItem();
        }

        private JetEvent wrap(Object item) {
            return jetEvent(item, key, JetEvent.NO_TIMESTAMP);
        }
    }
}
