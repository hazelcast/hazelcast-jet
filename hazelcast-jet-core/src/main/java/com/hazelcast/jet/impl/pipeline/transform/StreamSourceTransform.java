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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.WatermarkEmissionPolicy;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.core.WatermarkPolicy;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.pipeline.JetEventImpl;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.function.Function;

import static com.hazelcast.jet.core.WatermarkEmissionPolicy.suppressDuplicates;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLag;
import static java.util.Collections.emptyList;

/**
 * Javadoc pending.
 */
public class StreamSourceTransform<T> extends AbstractTransform implements StreamSource<T> {

    private static final long DEFAULT_IDLE_TIMEOUT = 2000L;
    private static final long DEFAULT_LAG = 1000L;

    private final Function<WatermarkGenerationParams<T>, ProcessorMetaSupplier> metaSupplierFn;

    private DistributedToLongFunction<T> timestampFn = t -> System.currentTimeMillis();
    private long idleTimeout = DEFAULT_IDLE_TIMEOUT;
    private DistributedSupplier<WatermarkPolicy> wmPolicy = limitingLag(DEFAULT_LAG);
    private WatermarkEmissionPolicy wmEmitPolicy = suppressDuplicates();

    private boolean hasWatermark = true;

    public StreamSourceTransform(
            @Nonnull String name,
            boolean supportsWatermarks,
            @Nonnull Function<WatermarkGenerationParams<T>, ProcessorMetaSupplier> metaSupplierFn
    ) {
        super(name, emptyList());
        this.hasWatermark = supportsWatermarks;
        this.metaSupplierFn = metaSupplierFn;
    }

    @Override
    public void addToDag(Planner p) {
        WatermarkGenerationParams<T> params = hasWatermark ?
                wmGenParams(timestampFn, JetEventImpl::jetEvent, wmPolicy, wmEmitPolicy, idleTimeout)
                :
                WatermarkGenerationParams.noWatermarks();
        p.addVertex(this, p.vertexName(name(), ""), metaSupplierFn.apply(params));
    }

    @Nonnull @Override
    public StreamSource<T> timestamp(@Nonnull DistributedToLongFunction<T> timestampFn) {
        assertWatermarksEnabled();
        this.timestampFn = timestampFn;
        return this;
    }

    @Nonnull @Override
    public StreamSource<T> idleTimeout(long idleTimeout) {
        assertWatermarksEnabled();
        this.idleTimeout = idleTimeout;
        return this;
    }

    @Nonnull @Override
    public StreamSource<T> watermarkPolicy(@Nonnull DistributedSupplier<WatermarkPolicy> wmPolicy) {
        assertWatermarksEnabled();
        this.wmPolicy = wmPolicy;
        return this;
    }

    @Nonnull @Override
    public StreamSource<T> watermarkEmissionPolicy(@Nonnull WatermarkEmissionPolicy wmEmitPolicy) {
        assertWatermarksEnabled();
        this.wmEmitPolicy = wmEmitPolicy;
        return this;
    }

    private void assertWatermarksEnabled() {
        Preconditions.checkTrue(hasWatermark, "This source does not support watermarks or" +
                " is configured not to emit watermarks");
    }

    @Nonnull @Override
    public StreamSource<T> noWatermarks() {
        hasWatermark = false;
        return this;
    }

    public boolean emitsJetEvents() {
        return hasWatermark;
    }

}
