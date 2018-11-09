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
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.pipeline.StreamSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.WatermarkGenerationParams.noWatermarks;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static java.util.Collections.emptyList;

public class StreamSourceTransform<T> extends AbstractTransform implements StreamSource<T> {

    private final Function<? super WatermarkGenerationParams<? super T>, ? extends ProcessorMetaSupplier> metaSupplierFn;
    private final boolean emitsWatermarks;

    @Nullable
    private WatermarkGenerationParams<? super T> wmParams;

    public StreamSourceTransform(
            @Nonnull String name,
            @Nullable WatermarkGenerationParams<? super T> initialWmParams,
            @Nonnull Function<? super WatermarkGenerationParams<? super T>, ? extends ProcessorMetaSupplier>
                    metaSupplierFn,
            boolean emitsWatermarks
    ) {
        super(name, emptyList());
        this.wmParams = initialWmParams;
        this.metaSupplierFn = metaSupplierFn;
        this.emitsWatermarks = emitsWatermarks;
    }

    public StreamSourceTransform(
            @Nonnull String name,
            @Nonnull Function<? super WatermarkGenerationParams<? super T>, ? extends ProcessorMetaSupplier>
                    metaSupplierFn,
            boolean emitsWatermarks
    ) {
        this(name, null, metaSupplierFn, emitsWatermarks);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addToDag(Planner p) {
        if (emitsWatermarks || wmParams == null) {
            // Reached when the source either emits both JetEvents and watermarks
            // or neither. In these cases we don't have to insert watermarks.
            p.addVertex(this, p.uniqueVertexName(name(), ""), localParallelism(),
                    metaSupplierFn.apply(wmParams != null ? wmParams : noWatermarks())
            );
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
            String v1name = p.uniqueVertexName(name(), "");
            Vertex v1 = p.dag.newVertex(v1name, metaSupplierFn.apply(wmParams))
                             .localParallelism(localParallelism());
            PlannerVertex pv2 = p.addVertex(
                    this, v1name + "-insertWM", localParallelism(), insertWatermarksP(wmParams)
            );
            p.dag.edge(between(v1, pv2.v).isolated());
        }
    }

    @Nullable
    public WatermarkGenerationParams<? super T> getWmParams() {
        return wmParams;
    }

    public void setWmGenerationParams(@Nonnull WatermarkGenerationParams<? super T> wmParams) {
        this.wmParams = wmParams;
    }

    public boolean emitsJetEvents() {
        return wmParams != null;
    }
}
