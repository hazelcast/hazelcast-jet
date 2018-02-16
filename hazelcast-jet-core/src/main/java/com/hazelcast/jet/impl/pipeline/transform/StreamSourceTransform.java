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
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.pipeline.JetEventImpl;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.function.Function;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.suppressDuplicates;
import static com.hazelcast.jet.core.WatermarkGenerationParams.noWatermarks;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLag;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static java.util.Collections.emptyList;

/**
 * Javadoc pending.
 */
public class StreamSourceTransform<T> extends AbstractTransform implements StreamSource<T> {

    private static final long DEFAULT_IDLE_TIMEOUT = 60000L;

    private final Function<WatermarkGenerationParams<T>, ProcessorMetaSupplier> metaSupplierFn;
    private final boolean supportsWatermarks;
    private DistributedToLongFunction<? super T> timestampFn;
    private long idleTimeout = DEFAULT_IDLE_TIMEOUT;
    private long maxLag;

    public StreamSourceTransform(
            @Nonnull String name,
            @Nonnull Function<WatermarkGenerationParams<T>, ProcessorMetaSupplier> metaSupplierFn,
            boolean supportsWatermarks
    ) {
        super(name, emptyList());
        this.metaSupplierFn = metaSupplierFn;
        this.supportsWatermarks = supportsWatermarks;
    }

    @Nonnull @Override
    public StreamSource<T> timestampWithSystemTime() {
        this.timestampFn = t -> System.currentTimeMillis();
        this.maxLag = 0;
        return this;
    }

    @Nonnull @Override
    public StreamSource<T> timestampWithEventTime(
            DistributedToLongFunction<? super T> timestampFn, long allowedLatenessMs
    ) {
        this.timestampFn = timestampFn;
        this.maxLag = allowedLatenessMs;
        return this;
    }

    @Nonnull @Override
    public StreamSource<T> setMaximumTimeBetweenEvents(long maxTimeMs) {
        assertWatermarksEnabled();
        this.idleTimeout = maxTimeMs;
        return this;
    }

    public boolean emitsJetEvents() {
        return timestampFn != null;
    }

    @Override
    public void addToDag(Planner p) {
        WatermarkGenerationParams<T> params = emitsJetEvents()
                ? wmGenParams(timestampFn, JetEventImpl::jetEvent, limitingLag(maxLag),
                                suppressDuplicates(), idleTimeout)
                : noWatermarks();
        if (supportsWatermarks || !emitsJetEvents()) {
            p.addVertex(this, p.vertexName(name(), ""), getLocalParallelism(), metaSupplierFn.apply(params));
        } else {
            //                  ------------
            //                 |  sourceP   |
            //                  ------------
            //                       |
            //                    isolated
            //                       v
            //                  -------------
            //                 |  insertWMP  |
            //                  -------------
            String v1name = p.vertexName(name(), "");
            Vertex v1 = p.dag.newVertex(v1name, metaSupplierFn.apply(params)).localParallelism(getLocalParallelism());
            PlannerVertex pv2 = p.addVertex(
                    this, v1name + "-insertWM", getLocalParallelism(), insertWatermarksP(params)
            );
            p.dag.edge(between(v1, pv2.v).isolated());
        }
    }
    private void assertWatermarksEnabled() {
        Preconditions.checkTrue(timestampFn != null, "This source does not support watermarks or" +
                " is configured not to emit watermarks");
    }
}
