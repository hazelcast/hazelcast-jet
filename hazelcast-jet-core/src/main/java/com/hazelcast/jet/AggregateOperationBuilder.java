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

        public <T> Step2<T, A> andAccumulate(DistributedBiConsumer<? super A, T> accumulateItemF) {
            return new Step2<>(this, accumulateItemF);
        }
    }

    public static class Step2<T, A> {
        final DistributedSupplier<A> createAccumulatorF;
        final DistributedBiConsumer<? super A, T> accumulateItemF;

        Step2(Step1<A> step1, DistributedBiConsumer<? super A, T> accumulateItemF) {
            this.createAccumulatorF = step1.createAccumulatorF;
            this.accumulateItemF = accumulateItemF;
        }

        public Step3<T, A> andCombine(DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF) {
           return new Step3<>(this, combineAccumulatorsF);
        }
    }

    public static class Step3<T, A> {
        private final DistributedSupplier<A> createAccumulatorF;
        private final DistributedBiConsumer<? super A, T> accumulateItemF;
        private final DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;

        Step3(Step2<T, A> step2, DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF) {
            this.createAccumulatorF = step2.createAccumulatorF;
            this.accumulateItemF = step2.accumulateItemF;
            this.combineAccumulatorsF = combineAccumulatorsF;
        }

        public Step4<T, A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductAccumulatorF) {
            return new Step4<>(this, deductAccumulatorF);
        }

        public <R> AggregateOperationImpl<T, A, R> andFinish(DistributedFunction<? super A, R> finishAccumulationF) {
            return new AggregateOperationImpl<>(createAccumulatorF, accumulateItemF,
                    combineAccumulatorsF, null, finishAccumulationF);
        }
    }

    public static class Step4<T, A> {
        private final DistributedSupplier<A> createAccumulatorF;
        private final DistributedBiConsumer<? super A, T> accumulateItemF;
        private final DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;
        private final DistributedBiConsumer<? super A, ? super A> deductAccumulatorF;

        Step4(Step3<T, A> step3, DistributedBiConsumer<? super A, ? super A> deductAccumulatorF) {
            this.createAccumulatorF = step3.createAccumulatorF;
            this.accumulateItemF = step3.accumulateItemF;
            this.combineAccumulatorsF = step3.combineAccumulatorsF;
            this.deductAccumulatorF = deductAccumulatorF;
        }

        public <R> AggregateOperationImpl<T, A, R> andFinish(DistributedFunction<? super A, R> finishAccumulationF) {
            return new AggregateOperationImpl<>(createAccumulatorF, accumulateItemF,
                    combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
        }
    }
}
