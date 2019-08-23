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

import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class TimestampTransform<T> extends AbstractTransform {
    @Nonnull
    private EventTimePolicy<? super T> eventTimePolicy;

    public TimestampTransform(
            @Nonnull Transform upstream,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        super("add-timestamps", upstream);
        this.eventTimePolicy = eventTimePolicy;
        checkNotNull(eventTimePolicy.timestampFn(),
                "timestampFn must not be null if timestamps aren't added in the source");
    }

    @Override
    public void localParallelism(int localParallelism) {
        throw new UnsupportedOperationException("Explicit local parallelism for addTimestamps() is not supported");
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex upstream = p.xform2vertex.get(this.upstream().get(0));
        int localParallelism = upstream.v.determineLocalParallelism(upstream.v.getLocalParallelism());
        PlannerVertex pv = p.addVertex(
                this, name(), localParallelism, insertWatermarksP(eventTimePolicy)
        );
        p.addEdges(this, pv.v, Edge::isolated);
    }

    @Nonnull
    public EventTimePolicy<? super T> getEventTimePolicy() {
        return eventTimePolicy;
    }

    public void setEventTimePolicy(@Nonnull EventTimePolicy<? super T> eventTimePolicy) {
        this.eventTimePolicy = eventTimePolicy;
    }
}
