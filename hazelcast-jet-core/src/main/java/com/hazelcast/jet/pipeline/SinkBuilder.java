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
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.connector.WriteBufferedP;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.util.Preconditions;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;

/**
 * Convenience for building custom {@link Sink} implementations on Pipeline API.
 *
 * @param <T> the type of the data the sink will receive
 * @param <S> the type of sink object
 */
public class SinkBuilder<T, S> {

    private DistributedFunction<JetInstance, S> createFn;
    private DistributedBiConsumer<S, T> addItemFn;
    private DistributedConsumer<S> flushFn;
    private DistributedConsumer<S> destroyFn;

    SinkBuilder() {
    }

    /**
     * Creates and returns a custom sink that consumes items from the pipeline
     *
     * @param <T> the type of the data the sink will receive
     */
    public <T> Sink<T> build() {
        Preconditions.checkNotNull(createFn, "createFn cannot be null");
        Preconditions.checkNotNull(addItemFn, "addItemFn cannot be null");
        if (flushFn == null) {
            flushFn = noopConsumer();
        }
        if (destroyFn == null) {
            destroyFn = noopConsumer();
        }

        ProcessorSupplier supplier = WriteBufferedP.supplier(createFn, addItemFn, flushFn, destroyFn);
        return new SinkImpl<>("custom-sink", preferLocalParallelismOne(supplier));
    }

    /**
     * Sets the sink object creation function
     *
     * @param createSinkFn a function that takes the {@link JetInstance}
     *                     and creates the sink. The returned sink object will be
     *                     passed to the following {@code addItemFn}, {@code flushFn}
     *                     and {@code destroyFn} functions.
     */
    public SinkBuilder<T, S> createFn(DistributedFunction<JetInstance, S> createSinkFn) {
        this.createFn = createSinkFn;
        return this;
    }

    /**
     * Sets the item addition to sink function
     *
     * @param addToSinkFn A bi-consumer that should implement the sink logic by
     *                    taking sink object and an item from the pipeline.
     *                    The logic should consume the item by adding it to the sink.
     */
    public SinkBuilder<T, S> addItemFn(DistributedBiConsumer<S, T> addToSinkFn) {
        this.addItemFn = addToSinkFn;
        return this;
    }

    /**
     * Sets the sink flush function
     *
     * @param flushSinkFn An optional consumer that can be used to implement
     *                    flushing logic.
     */
    public SinkBuilder<T, S> flushFn(DistributedConsumer<S> flushSinkFn) {
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
    public SinkBuilder<T, S> destroyFn(DistributedConsumer<S> destroySinkFn) {
        this.destroyFn = destroySinkFn;
        return this;
    }

}
