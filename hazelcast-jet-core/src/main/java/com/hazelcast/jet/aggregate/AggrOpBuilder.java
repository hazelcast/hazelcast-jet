/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.aggregate;

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.aggregate.AggregateOperation1Impl;
import com.hazelcast.jet.impl.aggregate.AggregateOperation2Impl;
import com.hazelcast.jet.impl.aggregate.AggregateOperation3Impl;
import com.hazelcast.jet.impl.aggregate.AggregateOperationImpl;
import com.hazelcast.jet.pipeline.bag.Tag;

import java.util.HashMap;
import java.util.Map;

/**
 * Javadoc pending.
 */
public final class AggrOpBuilder {

    private AggrOpBuilder() {
    }

    public static class Step1<A> {
        private final DistributedSupplier<A> createAccumulatorF;

        Step1(DistributedSupplier<A> createAccumulatorF) {
            this.createAccumulatorF = createAccumulatorF;
        }

        public <T1> Step2Arity1<T1, A> andAccumulate(DistributedBiConsumer<? super A, T1> accumulateItemF) {
            return new Step2Arity1<>(createAccumulatorF, accumulateItemF);
        }

        public <T1> Step2Arity1<T1, A> andAccumulate1(DistributedBiConsumer<? super A, T1> accumulateItemF1) {
            return new Step2Arity1<>(createAccumulatorF, accumulateItemF1);
        }

        public <T> Step2VarArity<A> andAccumulate(Tag<T> tag, DistributedBiConsumer<? super A, T> accumulateItemF) {
            return new Step2VarArity<>(createAccumulatorF, tag, accumulateItemF);
        }
    }

    public static class Step2Arity1<T1, A> {
        private final DistributedSupplier<A> createAccumulatorF;
        private final DistributedBiConsumer<? super A, T1> accumulateItemF1;
        private DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;
        private DistributedBiConsumer<? super A, ? super A> deductAccumulatorF;

        Step2Arity1(DistributedSupplier<A> createAccumulatorF, DistributedBiConsumer<? super A, T1> accumulateItemF1) {
            this.createAccumulatorF = createAccumulatorF;
            this.accumulateItemF1 = accumulateItemF1;
        }

        public <T2> Step2Arity2<T1, T2, A> andAccumulate2(DistributedBiConsumer<? super A, T2> accumulateItemF2) {
            return new Step2Arity2<>(this, accumulateItemF2);
        }

        public Step2Arity1<T1, A> andCombine(DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF) {
            this.combineAccumulatorsF = combineAccumulatorsF;
            return this;
        }

        public Step2Arity1<T1, A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductAccumulatorF) {
            this.deductAccumulatorF = deductAccumulatorF;
            return this;
        }

        public <R> AggregateOperation1<T1, A, R> andFinish(DistributedFunction<? super A, R> finishAccumulationF) {
            return new AggregateOperation1Impl<>(createAccumulatorF, accumulateItemF1,
                    combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
        }
    }

    public static class Step2Arity2<T1, T2, A> {
        private final DistributedSupplier<A> createAccumulatorF;
        private final DistributedBiConsumer<? super A, T1> accumulateItemF1;
        private final DistributedBiConsumer<? super A, T2> accumulateItemF2;
        private DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;
        private DistributedBiConsumer<? super A, ? super A> deductAccumulatorF;

        Step2Arity2(Step2Arity1<T1, A> step1, DistributedBiConsumer<? super A, T2> accumulateItemF2) {
            this.createAccumulatorF = step1.createAccumulatorF;
            this.accumulateItemF1 = step1.accumulateItemF1;
            this.accumulateItemF2 = accumulateItemF2;
        }

        public <T3> Step2Arity3<T1, T2, T3, A> andAccumulate3(DistributedBiConsumer<? super A, T3> accumulateItemF3) {
            return new Step2Arity3<>(this, accumulateItemF3);
        }

        public Step2Arity2<T1, T2, A> andCombine(DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF) {
            this.combineAccumulatorsF = combineAccumulatorsF;
            return this;
        }

        public Step2Arity2<T1, T2, A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductAccumulatorF) {
            this.deductAccumulatorF = deductAccumulatorF;
            return this;
        }

        public <R> AggregateOperation2<T1, T2, A, R> andFinish(DistributedFunction<? super A, R> finishAccumulationF) {
            return new AggregateOperation2Impl<>(createAccumulatorF,
                    accumulateItemF1, accumulateItemF2,
                    combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
        }
    }

    public static class Step2Arity3<T1, T2, T3, A> {
        private final DistributedSupplier<A> createAccumulatorF;
        private final DistributedBiConsumer<? super A, T1> accumulateItemF1;
        private final DistributedBiConsumer<? super A, T2> accumulateItemF2;
        private final DistributedBiConsumer<? super A, T3> accumulateItemF3;
        private DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;
        private DistributedBiConsumer<? super A, ? super A> deductAccumulatorF;

        Step2Arity3(Step2Arity2<T1, T2, A> step2,
                    DistributedBiConsumer<? super A, T3> accumulateItemF3
        ) {
            this.createAccumulatorF = step2.createAccumulatorF;
            this.accumulateItemF1 = step2.accumulateItemF1;
            this.accumulateItemF2 = step2.accumulateItemF2;
            this.accumulateItemF3 = accumulateItemF3;
        }

        public Step2Arity3<T1, T2, T3, A> andCombine(DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF) {
            this.combineAccumulatorsF = combineAccumulatorsF;
            return this;
        }

        public Step2Arity3<T1, T2, T3, A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductAccumulatorF) {
            this.deductAccumulatorF = deductAccumulatorF;
            return this;
        }

        public <R> AggregateOperation3<T1, T2, T3, A, R> andFinish(
                DistributedFunction<? super A, R> finishAccumulationF
        ) {
            return new AggregateOperation3Impl<>(createAccumulatorF,
                    accumulateItemF1, accumulateItemF2, accumulateItemF3,
                    combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
        }
    }

    public static class Step2VarArity<A> {
        private final DistributedSupplier<A> createAccumulatorF;
        private final Map<Tag, DistributedBiConsumer<? super A, ?>> accumulatorsByTag = new HashMap<>();
        private DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;
        private DistributedBiConsumer<? super A, ? super A> deductAccumulatorF;

        <T> Step2VarArity(
                DistributedSupplier<A> createAccumulatorF,
                Tag<T> tag,
                DistributedBiConsumer<? super A, T> accumulateItemF
        ) {
            this.createAccumulatorF = createAccumulatorF;
            accumulatorsByTag.put(tag, accumulateItemF);
        }

        public <T> Step2VarArity<A> andAccumulate(Tag<T> tag, DistributedBiConsumer<? super A, T> accumulateItemF) {
            accumulatorsByTag.put(tag, accumulateItemF);
            return this;
        }

        public Step2VarArity<A> andCombine(DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF) {
            this.combineAccumulatorsF = combineAccumulatorsF;
            return this;
        }

        public Step2VarArity<A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductAccumulatorF) {
            this.deductAccumulatorF = deductAccumulatorF;
            return this;
        }

        public <R> AggregateOperation<A, R> andFinish(DistributedFunction<? super A, R> finishAccumulationF) {
            return new AggregateOperationImpl<>(createAccumulatorF, accumulatorsByTag,
                    combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
        }
    }
}
