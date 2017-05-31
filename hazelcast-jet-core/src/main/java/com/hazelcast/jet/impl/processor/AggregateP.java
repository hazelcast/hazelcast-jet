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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.AggregateOperation;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;

/**
 * Batch processor that computes the supplied aggregate operation
 * on all received items.
 */
public class AggregateP<T, A, R> extends AbstractProcessor {
    private final DistributedBiConsumer<? super A, ? super T> accumulateF;
    private final DistributedFunction<? super A, R> finishF;
    private final A acc;
    private R result;

    public AggregateP(
            @Nonnull AggregateOperation<? super T, A, R> aggregateOperation
    ) {
        this.accumulateF = aggregateOperation.accumulateItemF();
        this.finishF = aggregateOperation.finishAccumulationF();
        this.acc = aggregateOperation.createAccumulatorF().get();
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        accumulateF.accept(acc, (T) item);
        return true;
    }

    @Override
    public boolean complete() {
        if (result == null) {
            result = finishF.apply(acc);
        }
        return tryEmit(result);
    }
}
