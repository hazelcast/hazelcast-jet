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

import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import java.util.Random;

import static com.hazelcast.jet.core.processor.Processors.mapUsingServiceP;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;

public class RepartitionTransform<T> extends AbstractTransform {

    private final boolean global;

    public RepartitionTransform(
            @Nonnull Transform upstream,
            boolean global
    ) {
        super("repartition" + (global ? "Global" : "Local"), upstream);
        this.global = global;
    }

    @Override
    public void addToDag(Planner p) {
        ServiceFactory<Random> serviceFactory = ServiceFactory.withCreateFn(jet -> new Random());
        PlannerVertex pv = p.addVertex(this, name(), localParallelism(),
                mapUsingServiceP(serviceFactory, (Random random, JetEvent item) ->
                        jetEvent(item.payload(), random.nextInt(), item.timestamp())));
        p.addEdges(this, pv.v, e -> {
            if (global) {
                e.distributed();
            }
        });
    }
}
