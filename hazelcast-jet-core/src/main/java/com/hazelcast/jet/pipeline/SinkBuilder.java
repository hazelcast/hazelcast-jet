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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;

/**
 * Convenience for building custom {@link Sink} implementations on Pipeline API.
 *
 * @param <T> the type of the data the sink will receive
 * @param <S> the type of sink object
 */
public final class SinkBuilder<T, S> {

    private final DistributedFunction<JetInstance, S> createFn;
    private DistributedBiConsumer<S, T> addItemFn;
    private DistributedConsumer<S> flushFn = noopConsumer();
    private DistributedConsumer<S> destroyFn = noopConsumer();

    SinkBuilder(@Nonnull DistributedFunction<JetInstance, S> createFn) {
        this.createFn = createFn;
    }

    /**
     * Creates and returns a custom sink that consumes items from the pipeline
     */
    public Sink<T> build() {
        Preconditions.checkNotNull(addItemFn, "addItemFn must be set");

        // local copy for serialization
        final DistributedFunction<JetInstance, S> createFn = this.createFn;
        ProcessorSupplier supplier = SinkProcessors.writeBufferedP(
                ctx -> createFn.apply(ctx.jetInstance()),
                addItemFn,
                flushFn,
                destroyFn
        );
        return new SinkImpl<>("custom-sink", preferLocalParallelismOne(supplier));
    }

    /**
     * Sets the item addition to sink function
     *
     * @param addToSinkFn A bi-consumer that should implement the sink logic by
     *                    taking sink object and an item from the pipeline.
     *                    The logic should consume the item by adding it to the sink.
     */
    public SinkBuilder<T, S> addItemFn(@Nonnull DistributedBiConsumer<S, T> addToSinkFn) {
        this.addItemFn = addToSinkFn;
        return this;
    }

    /**
     * Sets the sink flush function
     *
     * @param flushSinkFn An optional consumer that can be used to implement
     *                    flushing logic.
     */
    public SinkBuilder<T, S> flushFn(@Nonnull DistributedConsumer<S> flushSinkFn) {
        this.flushFn = flushSinkFn;
        return this;
    }

    /**
     * Sets the sink destroy function
     *
     * @param destroySinkFn An optional consumer that can be used to implement
     *                      destroy logic to clean-up resources allocated when
     *                      creating the sink object.
     */
    public SinkBuilder<T, S> destroyFn(@Nonnull DistributedConsumer<S> destroySinkFn) {
        this.destroyFn = destroySinkFn;
        return this;
    }

}
