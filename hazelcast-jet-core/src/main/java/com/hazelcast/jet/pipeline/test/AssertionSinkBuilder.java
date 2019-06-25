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

import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.ConsumerEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.impl.pipeline.test.AssertionP;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 *
 * @param <A>
 * @param <T>
 *
 * @since 3.2
 */
@Beta
public final class AssertionSinkBuilder<A, T> {

    private final SupplierEx<? extends A> createFn;
    private final String name;
    private BiConsumerEx<? super A, ? super T> receiveFn;
    private ConsumerEx<? super A> completeFn = ConsumerEx.noop();

    private AssertionSinkBuilder(
            @Nonnull String name,
            @Nonnull SupplierEx<? extends A> createFn
    ) {
        checkSerializable(createFn, "createFn");
        this.name = name;
        this.createFn = createFn;
    }

    /**
     *
     * @param name
     * @param createFn
     * @param <A>
     * @return
     */
    @Nonnull
    public static <A> AssertionSinkBuilder<A, Void> assertionSink(
            @Nonnull String name,
            @Nonnull SupplierEx<? extends A> createFn
    ) {
        return new AssertionSinkBuilder<>(name, createFn);
    }

    /**
     *
     * @param receiveFn
     * @param <T_NEW>
     * @return
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T_NEW> AssertionSinkBuilder<A, T_NEW> receiveFn(
            @Nonnull BiConsumerEx<? super A, ? super T_NEW> receiveFn
    ) {
        checkSerializable(receiveFn, "receiveFn");
        AssertionSinkBuilder<A, T_NEW> newThis = (AssertionSinkBuilder<A, T_NEW>) this;
        newThis.receiveFn = receiveFn;
        return newThis;
    }

    /**
     *
     * @param completeFn
     * @return
     */
    @Nonnull
    public AssertionSinkBuilder<A, T> completeFn(@Nonnull ConsumerEx<? super A> completeFn) {
        checkSerializable(completeFn, "completeFn");
        this.completeFn = completeFn;
        return this;
    }

    /**
     * Creates and returns the {@link Sink} with the components you supplied to
     * this builder.
     */
    @Nonnull
    public Sink<T> build() {
        Preconditions.checkNotNull(receiveFn, "receiveFn must be set");
        return new SinkImpl<>(name, AssertionP.assertionP(name, createFn, receiveFn, completeFn), true);
    }
}
