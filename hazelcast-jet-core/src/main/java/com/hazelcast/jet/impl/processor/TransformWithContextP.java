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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;

/**
 * Processor which, for each received item, emits all the items from the
 * traverser returned by the given item-to-traverser function, using a context
 * object.
 *
 * @param <C> context object type
 * @param <T> received item type
 * @param <R> emitted item type
 */
public class TransformWithContextP<C, T, R> extends AbstractProcessor implements Closeable {

    private final DistributedFunction<Context, ? extends C> createContextFn;
    private final DistributedBiFunction<? super C, T, ? extends Traverser<? extends R>> flatMapFn;
    private final DistributedConsumer<? super C> destroyContextFn;

    private C contextObject;
    private Traverser<? extends R> outputTraverser;

    /**
     * Constructs a processor with the given mapping function.
     */
    public TransformWithContextP(
            @Nonnull DistributedFunction<Context, ? extends C> createContextFn,
            @Nonnull DistributedBiFunction<C, T, ? extends Traverser<? extends R>> flatMapFn,
            @Nonnull DistributedConsumer<? super C> destroyContextFn
    ) {
        this.createContextFn = createContextFn;
        this.flatMapFn = flatMapFn;
        this.destroyContextFn = destroyContextFn;
    }

    @Override
    protected void init(@Nonnull Context context) {
        contextObject = createContextFn.apply(context);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (outputTraverser == null) {
            outputTraverser = flatMapFn.apply(contextObject, (T) item);
        }
        if (emitFromTraverser(outputTraverser)) {
            outputTraverser = null;
            return true;
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        destroyContextFn.accept(contextObject);
        contextObject = null;
    }
}
