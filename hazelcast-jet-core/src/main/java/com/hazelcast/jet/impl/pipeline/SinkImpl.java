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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.impl.pipeline.transform.AbstractTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.Sink;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.BitSet;

import static com.hazelcast.jet.impl.pipeline.FunctionAdapter.adaptingMetaSupplier;


public class SinkImpl<T> implements Sink<T> {

    @Nonnull
    private final String name;
    @Nonnull
    private ProcessorMetaSupplier metaSupplier;

    public SinkImpl(@Nonnull String name, @Nonnull ProcessorMetaSupplier metaSupplier) {
        this.name = name;
        this.metaSupplier = metaSupplier;
    }

    @Nonnull
    public ProcessorMetaSupplier metaSupplier() {
        return metaSupplier;
    }

    @Override
    public String name() {
        return name;
    }
}
