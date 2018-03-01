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

import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;

public class TimestampTransform<T> extends AbstractTransform {
    @Nonnull
    public WatermarkGenerationParams wmGenParams;

    public TimestampTransform(
            @Nonnull Transform upstream,
            @Nonnull WatermarkGenerationParams wmGenParams
    ) {
        super("timestamp", upstream);
        this.wmGenParams = wmGenParams;
    }

    @Override
    public void addToDag(Planner p) {
        @SuppressWarnings("unchecked")
        PlannerVertex pv = p.addVertex(
                this, p.uniqueVertexName(name(), ""), localParallelism(), insertWatermarksP(wmGenParams)
        );
        p.addEdges(this, pv.v);
    }

    @Nonnull
    public WatermarkGenerationParams getWmGenParams() {
        return wmGenParams;
    }

    public void setWmGenerationParams(@Nonnull WatermarkGenerationParams wmGenParams) {
        this.wmGenParams = wmGenParams;
    }
}
