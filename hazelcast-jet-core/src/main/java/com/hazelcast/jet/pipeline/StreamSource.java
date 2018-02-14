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

import com.hazelcast.jet.core.WatermarkEmissionPolicy;
import com.hazelcast.jet.core.WatermarkPolicy;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;

import javax.annotation.Nonnull;

/**
 * Javadoc pending.
 */
public interface StreamSource<T> {

    /**
     * Javadoc pending.
     */
    @Nonnull
    String name();

    /**
     * Javadoc pending.
     */
    @Nonnull
    StreamSource<T> timestampFn(@Nonnull DistributedToLongFunction<T> timestampFn);

    /**
     * Javadoc pending.
     */
    @Nonnull
    StreamSource<T> idleTimeout(long idleTimeout);

    /**
     * Javadoc pending.
     */
    @Nonnull
    StreamSource<T> wmPolicy(@Nonnull DistributedSupplier<WatermarkPolicy> wmPolicy);

    /**
     * Javadoc pending.
     */
    @Nonnull
    StreamSource<T> wmEmissionPolicy(@Nonnull WatermarkEmissionPolicy wmEmitPolicy);


}
