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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Partitioner;
import com.hazelcast.jet.impl.pipeline.JetEventFunctionAdapter;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.impl.pipeline.SinkImpl;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.jet.impl.pipeline.JetEventFunctionAdapter.inputAdaptingMetaSupplier;

public class SinkTransform<T> extends AbstractTransform {
    private final SinkImpl sink;

    public SinkTransform(@Nonnull SinkImpl sink, @Nonnull List<Transform> upstream) {
        super(sink.name(), upstream);
        this.sink = sink;
    }

    public SinkTransform(@Nonnull SinkImpl sink, @Nonnull Transform upstream) {
        super(sink.name(), upstream);
        this.sink = sink;
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, name(), localParallelism(),
                inputAdaptingMetaSupplier(sink.metaSupplier()));
        p.addEdges(this, pv.v, (e, ord) -> {
            // note: have to use an all-to-one edge for the assertion sink.
            // all the items will be routed to the member with the partition key
            if (sink.isTotalParallelismOne()) {
                e.allToOne(sink.name()).distributed();
            } else if (sink.inputPartitionKeyFunction() != null) {
                FunctionEx keyFn = JetEventFunctionAdapter.FN_ADAPTER.adaptKeyFn(sink.inputPartitionKeyFunction());
                e.partitioned(keyFn, Partitioner.defaultPartitioner());
            }
        });
    }
}
