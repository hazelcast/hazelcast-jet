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
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedTriFunction;
import com.hazelcast.jet.pipeline.JoinClause;

import javax.annotation.Nonnull;

import java.util.function.Function;

import static com.hazelcast.jet.impl.pipeline.JetEventImpl.jetEvent;
import static com.hazelcast.jet.pipeline.JoinClause.onKeys;


public class FunctionAdapters {

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
    <K> DistributedFunction<?, K> adaptKeyFn(@Nonnull DistributedFunction<?, K> keyFn) {
        return keyFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    DistributedBiConsumer<?, ?> adaptAccumulateFn(@Nonnull DistributedBiConsumer accumulateFn) {
        return accumulateFn;
    }

    @Nonnull
    AggregateOperation adaptAggregateOperation(@Nonnull AggregateOperation aggrOp) {
        return aggrOp;
    }

    @Nonnull
    AggregateOperation1 adaptAggregateOperation1(@Nonnull AggregateOperation1 aggrOp) {
        return aggrOp;
    }

    @Nonnull
    public JoinClause adaptJoinClause(@Nonnull JoinClause joinClause) {
        return joinClause;
    }

    @SuppressWarnings("unchecked")
    public <T, T1, R> DistributedBiFunction<Object, T1, Object> adaptMapToOutputFn(
            DistributedBiFunction<T, T1, R> mapToOutputFn
    ) {
        return (DistributedBiFunction<Object, T1, Object>) mapToOutputFn;
    }

    @SuppressWarnings("unchecked")
    <T, T1, T2, R> DistributedTriFunction<Object, T1, T2, Object> adaptMapToOutputFn(
            DistributedTriFunction<T, T1, T2, R> mapToOutputFn
    ) {
        return (DistributedTriFunction<Object, T1, T2, Object>) mapToOutputFn;
    }
}

class JetEventFunctionAdapters extends FunctionAdapters {
    @Nonnull @Override
    @SuppressWarnings("unchecked")
    DistributedFunction adaptMapFn(@Nonnull DistributedFunction mapFn) {
        return e -> jetEvent(((JetEvent) e).timestamp(), mapFn.apply(((JetEvent) e).payload()));
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
        return e -> rawFn.apply(((JetEvent) e).payload()).map(r -> jetEvent(((JetEvent) e).timestamp(), r));
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    DistributedFunction adaptKeyFn(@Nonnull DistributedFunction keyFn) {
        return e -> keyFn.apply(((JetEvent) e).payload());
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    DistributedBiConsumer adaptAccumulateFn(@Nonnull DistributedBiConsumer accumulateFn) {
        return (Object acc, Object e) -> accumulateFn.accept(acc, ((JetEvent) e).payload());
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    AggregateOperation adaptAggregateOperation(@Nonnull AggregateOperation aggrOp) {
        int arity = aggrOp.arity();
        DistributedBiConsumer[] adaptedAccumulateFns = new DistributedBiConsumer[arity];
        for (int i = 0; i < arity; i++) {
            adaptedAccumulateFns[i] = adaptAccumulateFn(aggrOp.accumulateFn(i));
        }

        return aggrOp;
    }

    @Nonnull
    AggregateOperation1 adaptAggregateOperation1(@Nonnull AggregateOperation1 aggrOp) {
        return aggrOp;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public JoinClause adaptJoinClause(@Nonnull JoinClause joinClause) {
        return onKeys(adaptKeyFn(joinClause.leftKeyFn()), joinClause.rightKeyFn())
                .projecting(joinClause.rightProjectFn());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, T1, R> DistributedBiFunction<Object, T1, Object> adaptMapToOutputFn(
            DistributedBiFunction<T, T1, R> mapToOutputFn
    ) {
        return (e, t1) -> {
            JetEvent<T> jetEvent = (JetEvent) e;
            return jetEvent(jetEvent.timestamp(), mapToOutputFn.apply(jetEvent.payload(), t1));
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    <T, T1, T2, R> DistributedTriFunction<Object, T1, T2, Object> adaptMapToOutputFn(
            DistributedTriFunction<T, T1, T2, R> mapToOutputFn
    ) {
        return (e, t1, t2) -> {
            JetEvent<T> jetEvent = (JetEvent) e;
            return jetEvent(jetEvent.timestamp(), mapToOutputFn.apply(jetEvent.payload(), t1, t2));
        };
    }
}
