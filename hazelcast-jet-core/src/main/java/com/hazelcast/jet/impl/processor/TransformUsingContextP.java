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
import com.hazelcast.jet.pipeline.TransformContext;

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
public class TransformUsingContextP<C, T, R> extends AbstractProcessor implements Closeable {

    private final TransformContext<C> transformContext;
    private final DistributedBiFunction<? super C, T, ? extends Traverser<? extends R>> flatMapFn;

    private C contextObject;
    private Traverser<? extends R> outputTraverser;

    /**
     * Constructs a processor with the given mapping function.
     */
    public TransformUsingContextP(
            @Nonnull TransformContext<C> transformContext,
            @Nonnull DistributedBiFunction<C, T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        this.transformContext = transformContext;
        this.flatMapFn = flatMapFn;
    }

    @Override
    protected void init(@Nonnull Context context) {
        contextObject = transformContext.getCreateFn().apply(context.jetInstance());
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
        transformContext.getDestroyFn().accept(contextObject);
        contextObject = null;
    }
}
