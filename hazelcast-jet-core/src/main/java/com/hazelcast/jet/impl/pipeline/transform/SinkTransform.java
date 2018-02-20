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
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.pipeline.Sink;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.function.Function;

public class SinkTransform<T> extends AbstractTransform implements Sink<T> {
    @Nonnull
    private ProcessorMetaSupplier metaSupplier;

    public SinkTransform(
            @Nonnull String name,
            @Nonnull ProcessorMetaSupplier metaSupplier
    ) {
        super(name, new ArrayList<>());
        this.metaSupplier = metaSupplier;
    }

    public void addUpstream(Transform upstream) {
        upstream().add(upstream);
    }

    @Nonnull
    public ProcessorMetaSupplier metaSupplier() {
        return metaSupplier;
    }

    public void replaceMetaSupplier(
            @Nonnull Function<? super ProcessorMetaSupplier, ? extends ProcessorMetaSupplier> updateMetaSupplierFn
    ) {
        metaSupplier = updateMetaSupplierFn.apply(metaSupplier);
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, p.uniqueVertexName(name(), ""), getLocalParallelism(), metaSupplier());
        p.addEdges(this, pv.v);
    }
}
