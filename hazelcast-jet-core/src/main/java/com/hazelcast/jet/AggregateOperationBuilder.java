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

package com.hazelcast.jet;

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.AggregateOperationImpl;

/**
 * Javadoc pending.
 */
public class AggregateOperationBuilder {

    public static class Step1<A> {
        final DistributedSupplier<A> createAccumulatorF;

        Step1(DistributedSupplier<A> createAccumulatorF) {
            this.createAccumulatorF = createAccumulatorF;
        }

        public <T1> Step2Arity1<T1, A> andAccumulate(DistributedBiConsumer<? super A, T1> accumulateItemF) {
            return new Step2Arity1<>(this, accumulateItemF);
        }

        public <T1> Step2Arity1<T1, A> andAccumulate1(DistributedBiConsumer<? super A, T1> accumulateItemF1) {
            return new Step2Arity1<>(this, accumulateItemF1);
        }
    }

    public static class Step2Arity1<T1, A> {
        final DistributedSupplier<A> createAccumulatorF;
        final DistributedBiConsumer<? super A, T1> accumulateItemF1;

        Step2Arity1(Step1<A> step1,
              DistributedBiConsumer<? super A, T1> accumulateItemF1
        ) {
            this.createAccumulatorF = step1.createAccumulatorF;
            this.accumulateItemF1 = accumulateItemF1;
        }

        public <T2> Step2Arity2<T1, T2, A> andAccumulate2(DistributedBiConsumer<? super A, T2> accumulateItemF2) {
            return new Step2Arity2<>(this, accumulateItemF2);
        }

        public Step3Arity1<T1, A> andCombine(DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF) {
            return new Step3Arity1<>(this, combineAccumulatorsF);
        }
    }

    public static class Step3Arity1<T1, A> {
        private final DistributedSupplier<A> createAccumulatorF;
        private final DistributedBiConsumer<? super A, T1> accumulateItemF1;
        private final DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;

        Step3Arity1(Step2Arity1<T1, A> step2, DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF) {
            this.createAccumulatorF = step2.createAccumulatorF;
            this.accumulateItemF1 = step2.accumulateItemF1;
            this.combineAccumulatorsF = combineAccumulatorsF;
        }

        public Step4Arity1<T1, A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductAccumulatorF) {
            return new Step4Arity1<>(this, deductAccumulatorF);
        }

        public <R> AggregateOperation<T1, A, R> andFinish(
                DistributedFunction<? super A, R> finishAccumulationF
        ) {
            return new AggregateOperationImpl<>(
                    createAccumulatorF, accumulateItemF1, combineAccumulatorsF, null, finishAccumulationF);
        }
    }

    public static class Step4Arity1<T1, A> {
        private final DistributedSupplier<A> createAccumulatorF;
        private final DistributedBiConsumer<? super A, T1> accumulateItemF1;
        private final DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;
        private final DistributedBiConsumer<? super A, ? super A> deductAccumulatorF;

        Step4Arity1(Step3Arity1<T1, A> step3, DistributedBiConsumer<? super A, ? super A> deductAccumulatorF) {
            this.createAccumulatorF = step3.createAccumulatorF;
            this.accumulateItemF1 = step3.accumulateItemF1;
            this.combineAccumulatorsF = step3.combineAccumulatorsF;
            this.deductAccumulatorF = deductAccumulatorF;
        }

        public <R> AggregateOperation<T1, A, R> andFinish(DistributedFunction<? super A, R> finishAccumulationF) {
            return new AggregateOperationImpl<>(createAccumulatorF, accumulateItemF1,
                    combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
        }
    }

    public static class Step2Arity2<T1, T2, A> {
        final DistributedSupplier<A> createAccumulatorF;
        final DistributedBiConsumer<? super A, T1> accumulateItemF1;
        final DistributedBiConsumer<? super A, T2> accumulateItemF2;

        Step2Arity2(Step2Arity1<T1, A> step1, DistributedBiConsumer<? super A, T2> accumulateItemF2) {
            this.createAccumulatorF = step1.createAccumulatorF;
            this.accumulateItemF1 = step1.accumulateItemF1;
            this.accumulateItemF2 = accumulateItemF2;
        }

        public Step3Arity2<T1, T2, A> andCombine(DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF) {
            return new Step3Arity2<>(this, combineAccumulatorsF);
        }
    }

    public static class Step3Arity2<T1, T2, A> {
        private final DistributedSupplier<A> createAccumulatorF;
        private final DistributedBiConsumer<? super A, T1> accumulateItemF1;
        private final DistributedBiConsumer<? super A, T2> accumulateItemF2;
        private final DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;

        Step3Arity2(Step2Arity2<T1, T2, A> step2Arity2,
                    DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF
        ) {
            this.createAccumulatorF = step2Arity2.createAccumulatorF;
            this.accumulateItemF1 = step2Arity2.accumulateItemF1;
            this.accumulateItemF2 = step2Arity2.accumulateItemF2;
            this.combineAccumulatorsF = combineAccumulatorsF;
        }

        public Step4Arity2<T1, T2, A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductAccumulatorF) {
            return new Step4Arity2<>(this, deductAccumulatorF);
        }

        public <R> AggregateOperation2<T1, T2, A, R> andFinish(
                DistributedFunction<? super A, R> finishAccumulationF
        ) {
            return new AggregateOperationImpl.Arity2<>(createAccumulatorF,
                    accumulateItemF1, accumulateItemF2,
                    combineAccumulatorsF, null, finishAccumulationF);
        }
    }

    public static class Step4Arity2<T1, T2, A> {
        private final DistributedSupplier<A> createAccumulatorF;
        private final DistributedBiConsumer<? super A, T1> accumulateItemF1;
        private final DistributedBiConsumer<? super A, T2> accumulateItemF2;
        private final DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;
        private final DistributedBiConsumer<? super A, ? super A> deductAccumulatorF;

        Step4Arity2(Step3Arity2<T1, T2, A> step3Arity2,
                    DistributedBiConsumer<? super A, ? super A> deductAccumulatorF
        ) {
            this.createAccumulatorF = step3Arity2.createAccumulatorF;
            this.accumulateItemF1 = step3Arity2.accumulateItemF1;
            this.accumulateItemF2 = step3Arity2.accumulateItemF2;
            this.combineAccumulatorsF = step3Arity2.combineAccumulatorsF;
            this.deductAccumulatorF = deductAccumulatorF;
        }

        public <R> AggregateOperation2<T1, T2, A, R> andFinish(
                DistributedFunction<? super A, R> finishAccumulationF
        ) {
            return new AggregateOperationImpl.Arity2<>(createAccumulatorF,
                    accumulateItemF1, accumulateItemF2,
                    combineAccumulatorsF, null, finishAccumulationF);
        }
    }

    public static class Step2Arity3<T1, T2, T3, A> {
        final DistributedSupplier<A> createAccumulatorF;
        final DistributedBiConsumer<? super A, T1> accumulateItemF1;
        final DistributedBiConsumer<? super A, T2> accumulateItemF2;
        final DistributedBiConsumer<? super A, T3> accumulateItemF3;

        Step2Arity3(Step1<A> step1,
                    DistributedBiConsumer<? super A, T1> accumulateItemF1,
                    DistributedBiConsumer<? super A, T2> accumulateItemF2,
                    DistributedBiConsumer<? super A, T3> accumulateItemF3
        ) {
            this.createAccumulatorF = step1.createAccumulatorF;
            this.accumulateItemF1 = accumulateItemF1;
            this.accumulateItemF2 = accumulateItemF2;
            this.accumulateItemF3 = accumulateItemF3;
        }

        public Step3Arity3<T1, T2, T3, A> andCombine(DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF) {
            return new Step3Arity3<>(this, combineAccumulatorsF);
        }
    }

    public static class Step3Arity3<T1, T2, T3, A> {
        private final DistributedSupplier<A> createAccumulatorF;
        private final DistributedBiConsumer<? super A, T1> accumulateItemF1;
        private final DistributedBiConsumer<? super A, T2> accumulateItemF2;
        private final DistributedBiConsumer<? super A, T3> accumulateItemF3;
        private final DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;

        Step3Arity3(Step2Arity3<T1, T2, T3, A> step2Arity3,
                    DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF
        ) {
            this.createAccumulatorF = step2Arity3.createAccumulatorF;
            this.accumulateItemF1 = step2Arity3.accumulateItemF1;
            this.accumulateItemF2 = step2Arity3.accumulateItemF2;
            this.accumulateItemF3 = step2Arity3.accumulateItemF3;
            this.combineAccumulatorsF = combineAccumulatorsF;
        }

        public Step4Arity3<T1, T2, T3, A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductAccumulatorF) {
            return new Step4Arity3<>(this, deductAccumulatorF);
        }

        public <R> AggregateOperation3<T1, T2, T3, A, R> andFinish(
                DistributedFunction<? super A, R> finishAccumulationF
        ) {
            return new AggregateOperationImpl.Arity3<>(createAccumulatorF,
                    accumulateItemF1, accumulateItemF2, accumulateItemF3,
                    combineAccumulatorsF, null, finishAccumulationF);
        }
    }

    public static class Step4Arity3<T1, T2, T3, A> {
        private final DistributedSupplier<A> createAccumulatorF;
        private final DistributedBiConsumer<? super A, T1> accumulateItemF1;
        private final DistributedBiConsumer<? super A, T2> accumulateItemF2;
        private final DistributedBiConsumer<? super A, T3> accumulateItemF3;
        private final DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;
        private final DistributedBiConsumer<? super A, ? super A> deductAccumulatorF;

        Step4Arity3(Step3Arity3<T1, T2, T3, A> step3Arity3,
                    DistributedBiConsumer<? super A, ? super A> deductAccumulatorF
        ) {
            this.createAccumulatorF = step3Arity3.createAccumulatorF;
            this.accumulateItemF1 = step3Arity3.accumulateItemF1;
            this.accumulateItemF2 = step3Arity3.accumulateItemF2;
            this.accumulateItemF3 = step3Arity3.accumulateItemF3;
            this.combineAccumulatorsF = step3Arity3.combineAccumulatorsF;
            this.deductAccumulatorF = deductAccumulatorF;
        }

        public <R> AggregateOperation3<T1, T2, T3, A, R> andFinish(
                DistributedFunction<? super A, R> finishAccumulationF
        ) {
            return new AggregateOperationImpl.Arity3<>(createAccumulatorF,
                    accumulateItemF1, accumulateItemF2, accumulateItemF3,
                    combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
        }
    }
}
