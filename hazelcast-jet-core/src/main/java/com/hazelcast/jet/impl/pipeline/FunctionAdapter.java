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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedTriFunction;
import com.hazelcast.jet.function.KeyedWindowResultFunction;
import com.hazelcast.jet.function.WindowResultFunction;
import com.hazelcast.jet.impl.aggregate.AggregateOperationImpl;
import com.hazelcast.jet.pipeline.JoinClause;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.function.Function;

import static com.hazelcast.jet.impl.pipeline.JetEventImpl.jetEvent;
import static com.hazelcast.jet.pipeline.JoinClause.onKeys;


public class FunctionAdapter {

    @Nonnull
    @SuppressWarnings("unchecked")
    DistributedFunction<?, ?> adaptMapFn(@Nonnull DistributedFunction mapFn) {
        return mapFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    DistributedPredicate<?> adaptFilterFn(@Nonnull DistributedPredicate filterFn) {
        return filterFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <R, T> DistributedFunction<? super Object, ? extends Traverser<?>> adaptFlatMapFn(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return (DistributedFunction<? super Object, ? extends Traverser<?>>) flatMapFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    DistributedBiConsumer<?, ?> adaptAccumulateFn(@Nonnull DistributedBiConsumer accumulateFn) {
        return accumulateFn;
    }

    @Nonnull
    public JoinClause adaptJoinClause(@Nonnull JoinClause joinClause) {
        return joinClause;
    }

    @SuppressWarnings("unchecked")
    public <T, T1, R> DistributedBiFunction<Object, T1, Object> adapthashJoinOutputFn(
            DistributedBiFunction<T, T1, R> mapToOutputFn
    ) {
        return (DistributedBiFunction<Object, T1, Object>) mapToOutputFn;
    }

    @SuppressWarnings("unchecked")
    <T, T1, T2, R> DistributedTriFunction<Object, T1, T2, Object> adapthashJoinOutputFn(
            DistributedTriFunction<T, T1, T2, R> mapToOutputFn
    ) {
        return (DistributedTriFunction<Object, T1, T2, Object>) mapToOutputFn;
    }

    <R, OUT> WindowResultFunction adaptWindowResultFn(
            WindowResultFunction<? super R, ? extends OUT> windowResultFn
    ) {
        return windowResultFn;
    }

    <K, R, OUT> KeyedWindowResultFunction adaptKeyedWindowResultFn(
            KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> keyedWindowResultFn
    ) {
        return keyedWindowResultFn;
    }
}

class JetEventFunctionAdapter extends FunctionAdapter {
    @Nonnull @Override
    @SuppressWarnings("unchecked")
    DistributedFunction adaptMapFn(@Nonnull DistributedFunction mapFn) {
        return e -> jetEvent(mapFn.apply(((JetEvent) e).payload()), ((JetEvent) e).timestamp());
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    DistributedPredicate adaptFilterFn(@Nonnull DistributedPredicate filterFn) {
        return e -> filterFn.test(((JetEvent) e).payload());
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    <R, T> DistributedFunction<? super Object, ? extends Traverser<?>> adaptFlatMapFn(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        DistributedFunction<Object, Traverser> rawFn = (DistributedFunction<Object, Traverser>) (Function) flatMapFn;
        return e -> rawFn.apply(((JetEvent) e).payload()).map(r -> jetEvent(r, ((JetEvent) e).timestamp()));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <T, K> DistributedFunction<? super JetEvent<T>, ? extends K> adaptKeyFn(
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn
    ) {
        return e -> keyFn.apply(e.payload());
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    DistributedBiConsumer adaptAccumulateFn(@Nonnull DistributedBiConsumer accumulateFn) {
        return (Object acc, Object e) -> accumulateFn.accept(acc, ((JetEvent) e).payload());
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    AggregateOperation adaptAggregateOperation(@Nonnull AggregateOperation aggrOp) {
        if (aggrOp instanceof AggregateOperation1) {
            AggregateOperation1 aggrOp1 = (AggregateOperation1) aggrOp;
            return aggrOp1
                    .withAccumulateFn(adaptAccumulateFn(aggrOp1.accumulateFn()));
        } else if (aggrOp instanceof AggregateOperation2) {
            AggregateOperation2 aggrOp2 = (AggregateOperation2) aggrOp;
            return aggrOp2
                    .withAccumulateFn0(adaptAccumulateFn(aggrOp2.accumulateFn0()))
                    .withAccumulateFn1(adaptAccumulateFn(aggrOp2.accumulateFn1()));
        } else if (aggrOp instanceof AggregateOperation3) {
            AggregateOperation3 aggrOp3 = (AggregateOperation3) aggrOp;
            return aggrOp3
                    .withAccumulateFn0(adaptAccumulateFn(aggrOp3.accumulateFn0()))
                    .withAccumulateFn1(adaptAccumulateFn(aggrOp3.accumulateFn1()))
                    .withAccumulateFn2(adaptAccumulateFn(aggrOp3.accumulateFn2()));
        } else {
            DistributedBiConsumer[] adaptedAccFns = new DistributedBiConsumer[aggrOp.arity()];
            Arrays.setAll(adaptedAccFns, i -> adaptAccumulateFn(aggrOp.accumulateFn(i)));
            return ((AggregateOperationImpl) aggrOp).withAccumulateFns(adaptedAccFns);
        }
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public JoinClause adaptJoinClause(@Nonnull JoinClause joinClause) {
        return onKeys(adaptKeyFn(joinClause.leftKeyFn()), joinClause.rightKeyFn())
                .projecting(joinClause.rightProjectFn());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, T1, R> DistributedBiFunction<Object, T1, Object> adapthashJoinOutputFn(
            DistributedBiFunction<T, T1, R> mapToOutputFn
    ) {
        return (e, t1) -> {
            JetEvent<T> jetEvent = (JetEvent) e;
            return jetEvent(mapToOutputFn.apply(jetEvent.payload(), t1), jetEvent.timestamp());
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    <T, T1, T2, R> DistributedTriFunction<Object, T1, T2, Object> adapthashJoinOutputFn(
            DistributedTriFunction<T, T1, T2, R> mapToOutputFn
    ) {
        return (e, t1, t2) -> {
            JetEvent<T> jetEvent = (JetEvent) e;
            return jetEvent(mapToOutputFn.apply(jetEvent.payload(), t1, t2), jetEvent.timestamp());
        };
    }

    @Override
    <R, OUT> WindowResultFunction<? super R, JetEvent<OUT>> adaptWindowResultFn(
            WindowResultFunction<? super R, ? extends OUT> windowResultFn
    ) {
        return (long winStart, long winEnd, R windowResult) ->
                jetEvent(windowResultFn.apply(winStart, winEnd, windowResult), winEnd);
    }

    @Override
    <K, R, OUT> KeyedWindowResultFunction<? super K, ? super R, JetEvent<OUT>> adaptKeyedWindowResultFn(
            KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> keyedWindowResultFn
    ) {
        return (long winStart, long winEnd, K key, R windowResult) ->
                jetEvent(keyedWindowResultFn.apply(winStart, winEnd, key, windowResult), winEnd);
    }
}
