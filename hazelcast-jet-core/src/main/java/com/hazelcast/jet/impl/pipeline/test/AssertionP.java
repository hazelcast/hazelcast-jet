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

package com.hazelcast.jet.pipeline.test;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.ConsumerEx;
import com.hazelcast.jet.function.SupplierEx;

import javax.annotation.Nonnull;

public class AssertionP<A, T> extends AbstractProcessor {

    private final SupplierEx<? extends A> createFn;
    private final BiConsumerEx<? super A, ? super T> receiveFn;
    private final ConsumerEx<? super A> completeFn;

    private A state;

    private AssertionP(SupplierEx<? extends A> createFn,
                       BiConsumerEx<? super A, ? super T> receiveFn,
                       ConsumerEx<? super A> completeFn
    ) {
        this.createFn = createFn;
        this.receiveFn = receiveFn;
        this.completeFn = completeFn;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);
        state = createFn.get();
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        receiveFn.accept(state, (T) item);
        return true;
    }

    @Override
    public boolean complete() {
        completeFn.accept(state);
        return true;
    }

    public static <A, T> ProcessorMetaSupplier assertionP(
        SupplierEx<? extends A> createFn,
        BiConsumerEx<? super A, ? super T> receiveFn,
        ConsumerEx<? super A> completeFn
    ) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(
            () -> new AssertionP<>(createFn, receiveFn, completeFn))
        );
    }
}
