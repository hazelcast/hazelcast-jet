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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.core.MetricsSupplier;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public class ProcessorWithMetricsMetaSupplier implements ProcessorMetaSupplier {

    private final Collection<MetricsSupplier> metricsSuppliers;
    private final ProcessorMetaSupplier underlying;

    public ProcessorWithMetricsMetaSupplier(@Nonnull Collection<MetricsSupplier> metricsSuppliers,
                                            @Nonnull ProcessorMetaSupplier underlying) {
        this.metricsSuppliers = metricsSuppliers;
        this.underlying = underlying;
    }

    public Collection<MetricsSupplier> getMetricsSuppliers() {
        return metricsSuppliers;
    }

    @Override
    public int preferredLocalParallelism() {
        return underlying.preferredLocalParallelism();
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        underlying.init(context);
    }

    @Nonnull
    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        return underlying.get(addresses);
    }

    @Override
    public void close(@Nullable Throwable error) throws Exception {
        underlying.close(error);
    }
}
