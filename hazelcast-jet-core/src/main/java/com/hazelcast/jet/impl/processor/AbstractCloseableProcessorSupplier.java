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

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedIntFunction;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Collection;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

public abstract class AbstractCloseableProcessorSupplier<E extends Processor & Closeable> implements ProcessorSupplier {

    private ILogger logger;
    private Collection<E> processors;

    @Override
    public final void init(@Nonnull Context context) {
        logger = context.logger();
    }

    @Nonnull @Override
    public final Collection<E> get(int count) {
        return processors = getProcessors(count);
    }

    @Override
    public final void complete(Throwable error) {
        Throwable firstError = null;
        // close all processors, ignoring their failures and throwing the first failure (if any)
        for (E p : processors) {
            try {
                p.close();
            } catch (Throwable e) {
                if (firstError == null) {
                    firstError = e;
                } else {
                    logger.severe(e);
                }
            }
        }
        if (firstError != null) {
            throw sneakyThrow(firstError);
        }
    }

    protected void initInternal(@Nonnull Context context) {
    }

    protected abstract Collection<E> getProcessors(int count);

    protected void completeInternal(Throwable error) {
    }

    public static <E extends Processor & Closeable> ProcessorSupplier closeableSupplier(
            DistributedIntFunction<Collection<E>> processorSupplier
    ) {
        return new AbstractCloseableProcessorSupplier<E>() {
            @Override
            protected Collection<E> getProcessors(int count) {
                return processorSupplier.apply(count);
            }
        };
    }
}
