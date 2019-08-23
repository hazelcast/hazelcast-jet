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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.BiPredicateEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.jet.function.ToLongFunctionEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.impl.processor.ProcessorWrapper;
import com.hazelcast.jet.impl.util.WrappingProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.JoinClause;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.JetEvent.jetEvent;

public class FunctionAdapter {

    @Nonnull
    public <T, K> FunctionEx<?, ? extends K> adaptKeyFn(@Nonnull FunctionEx<? super T, ? extends K> keyFn) {
        return keyFn;
    }

    @SuppressWarnings("unused")
    @Nonnull
    <T> ToLongFunctionEx<?> adaptTimestampFn() {
        return t -> Long.MIN_VALUE;
    }

    @Nonnull
    <T, R> FunctionEx<?, ?> adaptMapFn(@Nonnull FunctionEx<? super T, ? extends R> mapFn) {
        return mapFn;
    }

    @Nonnull
    <T> PredicateEx<?> adaptFilterFn(@Nonnull PredicateEx<? super T> filterFn) {
        return filterFn;
    }


    @Nonnull
    <T, R> FunctionEx<?, ? extends Traverser<?>> adaptFlatMapFn(
            @Nonnull FunctionEx<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return flatMapFn;
    }

    @Nonnull
    <S, T, R> BiFunctionEx<? super S, ?, ?> adaptStatefulMapFn(
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        return mapFn;
    }

    @Nonnull
    <S, T, R> BiFunctionEx<? super S, ?, ? extends Traverser<?>> adaptStatefulFlatMapFn(
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return flatMapFn;
    }

    @Nonnull
    <T, K, R, OUT> TriFunction<?, ? super K, ? super R, ?> adaptStatefulOutputFn(
            @Nonnull TriFunction<? super T, ? super K, ? super R, ? extends OUT> outputFn
    ) {
        return outputFn;
    }

    @Nonnull
    <C, T, R> BiFunctionEx<? super C, ?, ?> adaptMapUsingContextFn(
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends R> mapFn
    ) {
        return mapFn;
    }

    @Nonnull
    <C, T> BiPredicateEx<? super C, ?> adaptFilterUsingContextFn(
            @Nonnull BiPredicateEx<? super C, ? super T> filterFn
    ) {
        return filterFn;
    }

    @Nonnull
    <C, T, R> BiFunctionEx<? super C, ?, ? extends Traverser<?>> adaptFlatMapUsingContextFn(
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return flatMapFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, T, R> BiFunctionEx<? super C, ?, ? extends CompletableFuture<Traverser<?>>> adaptFlatMapUsingContextAsyncFn(
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        return (BiFunctionEx) flatMapAsyncFn;
    }

    @Nonnull
    <T, R extends CharSequence> FunctionEx<?, ? extends R> adaptToStringFn(
            @Nonnull FunctionEx<? super T, ? extends R> toStringFn
    ) {
        return toStringFn;
    }

    @Nonnull
    public <K, T0, T1, T1_OUT> JoinClause<? extends K, ?, ? super T1, ? extends T1_OUT>
    adaptJoinClause(@Nonnull JoinClause<? extends K, ? super T0, ? super T1, ? extends T1_OUT> joinClause) {
        return joinClause;
    }

    @Nonnull
    public <T, T1, R> BiFunctionEx<?, ? super T1, ?> adaptHashJoinOutputFn(
            @Nonnull BiFunctionEx<? super T, ? super T1, ? extends R> mapToOutputFn
    ) {
        return mapToOutputFn;
    }

    @Nonnull
    <T, T1, T2, R> TriFunction<?, ? super T1, ? super T2, ?> adaptHashJoinOutputFn(
            @Nonnull TriFunction<? super T, ? super T1, ? super T2, ? extends R> mapToOutputFn
    ) {
        return mapToOutputFn;
    }

    @Nonnull
    <A, R> AggregateOperation<A, ? extends R> adaptAggregateOperation(
            @Nonnull AggregateOperation<A, ? extends R> aggrOp
    ) {
        return aggrOp;
    }

    @Nonnull
    <T, A, R> AggregateOperation1<?, A, ? extends R> adaptAggregateOperation1(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        return aggrOp;
    }

    @Nonnull
    public static ProcessorMetaSupplier adaptingMetaSupplier(ProcessorMetaSupplier metaSup, int[] ordinalsToAdapt) {
        return new WrappingProcessorMetaSupplier(metaSup, p -> new AdaptingProcessor(p, ordinalsToAdapt));
    }

    private static final class AdaptingProcessor extends ProcessorWrapper {
        private final AdaptingInbox adaptingInbox = new AdaptingInbox();
        private final BitSet shouldAdaptOrdinal = new BitSet();

        AdaptingProcessor(Processor wrapped, int[] ordinalsToAdapt) {
            super(wrapped);
            for (int ordinal : ordinalsToAdapt) {
                shouldAdaptOrdinal.set(ordinal);
            }
        }

        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            Inbox inboxToUse;
            if (shouldAdaptOrdinal.get(ordinal)) {
                inboxToUse = adaptingInbox;
                adaptingInbox.setWrappedInbox(inbox);
            } else {
                inboxToUse = inbox;
            }
            super.process(ordinal, inboxToUse);
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
}

class JetEventFunctionAdapter extends FunctionAdapter {
    @Nonnull @Override
    public <T, K> FunctionEx<? super JetEvent<T>, ? extends K> adaptKeyFn(
            @Nonnull FunctionEx<? super T, ? extends K> keyFn
    ) {
        return e -> keyFn.apply(e.payload());
    }

    @Nonnull @Override
    <T> ToLongFunctionEx<? super JetEvent<T>> adaptTimestampFn() {
        return JetEvent::timestamp;
    }

    @Nonnull @Override
    <T, R> FunctionEx<? super JetEvent<T>, ?> adaptMapFn(
            @Nonnull FunctionEx<? super T, ? extends R> mapFn
    ) {
        return e -> jetEvent(e.timestamp(), mapFn.apply(e.payload()));
    }

    @Nonnull @Override
    <T> PredicateEx<? super JetEvent<T>> adaptFilterFn(@Nonnull PredicateEx<? super T> filterFn) {
        return e -> filterFn.test(e.payload());
    }

    @Nonnull @Override
    <T, R> FunctionEx<? super JetEvent<T>, ? extends Traverser<JetEvent<R>>> adaptFlatMapFn(
            @Nonnull FunctionEx<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return e -> flatMapFn.apply(e.payload()).map(r -> jetEvent(e.timestamp(), r));
    }

    @Nonnull @Override
    <S, T, R> BiFunctionEx<? super S, ? super JetEvent<T>, ? extends R> adaptStatefulMapFn(
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        return (state, e) -> mapFn.apply(state, e.payload());
    }

    @Nonnull @Override
    <S, T, R> BiFunctionEx<? super S, ? super JetEvent<T>, ? extends Traverser<R>> adaptStatefulFlatMapFn(
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return (state, e) -> flatMapFn.apply(state, e.payload());
    }

    @Nonnull @Override
    <T, K, R, OUT> TriFunction<? super JetEvent<T>, ? super K, ? super R, ? extends JetEvent<OUT>> adaptStatefulOutputFn(
            @Nonnull TriFunction<? super T, ? super K, ? super R, ? extends OUT> outputFn
    ) {
        return (event, key, r) -> jetEvent(event.timestamp(), outputFn.apply(event.payload(), key, r));
    }

    @Nonnull @Override
    <C, T, R> BiFunctionEx<? super C, ? super JetEvent<T>, ? extends JetEvent<R>> adaptMapUsingContextFn(
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends R> mapFn
    ) {
        return (context, e) -> jetEvent(e.timestamp(), mapFn.apply(context, e.payload()));
    }

    @Nonnull @Override
    <C, T> BiPredicateEx<? super C, ? super JetEvent<T>> adaptFilterUsingContextFn(
            @Nonnull BiPredicateEx<? super C, ? super T> filterFn
    ) {
        return (context, e) -> filterFn.test(context, e.payload());
    }

    @Nonnull @Override
    <C, T, R> BiFunctionEx<? super C, ? super JetEvent<T>, ? extends Traverser<JetEvent<R>>>
    adaptFlatMapUsingContextFn(
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return (context, e) -> flatMapFn.apply(context, e.payload()).map(r -> jetEvent(e.timestamp(), r));
    }

    @Nonnull @Override
    <C, T, R> BiFunctionEx<? super C, ?, ? extends CompletableFuture<Traverser<?>>>
    adaptFlatMapUsingContextAsyncFn(
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        return (C context, JetEvent<T> e) ->
                flatMapAsyncFn.apply(context, e.payload()).thenApply(trav -> trav.map(re -> jetEvent(e.timestamp(), re)));
    }

    @Nonnull @Override
    <T, STR extends CharSequence> FunctionEx<? super JetEvent<T>, ? extends STR> adaptToStringFn(
            @Nonnull FunctionEx<? super T, ? extends STR> toStringFn
    ) {
        return e -> toStringFn.apply(e.payload());
    }

    @Nonnull @Override
    public <K, T0, T1, T1_OUT> JoinClause<? extends K, ? super JetEvent<T0>, ? super T1, ? extends T1_OUT>
    adaptJoinClause(
            @Nonnull JoinClause<? extends K, ? super T0, ? super T1, ? extends T1_OUT> joinClause
    ) {
        return JoinClause.<K, JetEvent<T0>, T1>onKeys(adaptKeyFn(joinClause.leftKeyFn()), joinClause.rightKeyFn())
                .projecting(joinClause.rightProjectFn());
    }

    @Nonnull @Override
    public <T, T1, R> BiFunctionEx<? super JetEvent<T>, ? super T1, ?> adaptHashJoinOutputFn(
            @Nonnull BiFunctionEx<? super T, ? super T1, ? extends R> mapToOutputFn
    ) {
        return (e, t1) -> jetEvent(e.timestamp(), mapToOutputFn.apply(e.payload(), t1));
    }

    @Nonnull @Override
    <T, T1, T2, R> TriFunction<? super JetEvent<T>, ? super T1, ? super T2, ?> adaptHashJoinOutputFn(
            @Nonnull TriFunction<? super T, ? super T1, ? super T2, ? extends R> mapToOutputFn
    ) {
        return (e, t1, t2) -> jetEvent(e.timestamp(), mapToOutputFn.apply(e.payload(), t1, t2));
    }

    @Nonnull @Override
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

    @Nonnull @Override
    <T, A, R> AggregateOperation1<? super JetEvent<T>, A, ? extends R> adaptAggregateOperation1(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        return aggrOp.withAccumulateFn(adaptAccumulateFn(aggrOp.accumulateFn()));
    }

    @Nonnull
    static <T0, T1, A, R> AggregateOperation2<? super JetEvent<T0>, ? super JetEvent<T1>, A, ? extends R>
    adaptAggregateOperation2(@Nonnull AggregateOperation2<? super T0, ? super T1, A, ? extends R> aggrOp) {
        return aggrOp
                .<JetEvent<T0>>withAccumulateFn0(adaptAccumulateFn(aggrOp.accumulateFn0()))
                .withAccumulateFn1(adaptAccumulateFn(aggrOp.accumulateFn1()));
    }

    @Nonnull
    static <T0, T1, T2, A, R>
    AggregateOperation3<? super JetEvent<T0>, ? super JetEvent<T1>, ? super JetEvent<T2>, A, ? extends R>
    adaptAggregateOperation3(@Nonnull AggregateOperation3<? super T0, ? super T1, ? super T2, A, ? extends R> aggrOp) {
        return aggrOp
                .<JetEvent<T0>>withAccumulateFn0(adaptAccumulateFn(aggrOp.accumulateFn0()))
                .<JetEvent<T1>>withAccumulateFn1(adaptAccumulateFn(aggrOp.accumulateFn1()))
                .withAccumulateFn2(adaptAccumulateFn(aggrOp.accumulateFn2()));
    }

    @Nonnull
    private static <A, T> BiConsumerEx<? super A, ? super JetEvent<T>> adaptAccumulateFn(
            @Nonnull BiConsumerEx<? super A, ? super T> accumulateFn
    ) {
        return (acc, t) -> accumulateFn.accept(acc, t.payload());
    }
}
