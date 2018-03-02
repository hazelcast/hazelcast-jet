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

package com.hazelcast.jet.core;

import com.hazelcast.jet.function.DistributedIntFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Collection;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.stream.Collectors.toList;

/**
 * A {@link ProcessorSupplier} which closes the processor instances it
 * created when the job is complete. Useful when the processor uses
 * external resources such as connections or files.
 * <p>
 * The processors must implement {@link Closeable}. The call to {@code
 * close()} will be the very last call on the processor.
 *
 * @param <E> the processor type
 */
public class CloseableProcessorSupplier<E extends Processor & Closeable> implements ProcessorSupplier {

    static final long serialVersionUID = 1L;

    private DistributedIntFunction<Collection<E>> supplier;

    private transient ILogger logger;
    private transient Collection<E> processors;

    /**
     * Constructs an instance without a wrapped processor supplier. It must be
     * set later using {@link #setSupplier}.
     */
    public CloseableProcessorSupplier() {
    }

    /**
     * Constructs an instance which will wrap all the processors the provided
     * {@code simpleSupplier} returns.
     */
    public CloseableProcessorSupplier(DistributedSupplier<E> simpleSupplier) {
        this(count -> IntStream.range(0, count)
                               .mapToObj(i -> simpleSupplier.get())
                               .collect(toList()));
    }

    /**
     * Constructs an instance which will wrap all the processors the provided
     * {@code supplier} returns.
     *
     * @param supplier supplier of processors. The {@code int} parameter passed to it is the
     *                 number of processors that should be in the returned collection.
     */
    public CloseableProcessorSupplier(DistributedIntFunction<Collection<E>>  supplier) {
        this.supplier = supplier;
    }

    /**
     * Initializes this object with the given supplier. May only be invoked if
     * the supplier property hasn't been enitialized yet.
     */
    public void setSupplier(DistributedIntFunction<Collection<E>> newSupplier) {
        if (supplier != null) {
            throw new IllegalStateException("supplier already assigned");
        }
        supplier = newSupplier;
    }

    @Override
    public void init(@Nonnull Context context) {
        logger = context.logger();
    }

    @Nonnull @Override
    public Collection<E> get(int count) {
        assert processors == null;
        return processors = supplier.apply(count);
    }

    @Override
    public void complete(Throwable error) {
        if (processors == null) {
            return;
        }

        Throwable firstError = null;
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
}
