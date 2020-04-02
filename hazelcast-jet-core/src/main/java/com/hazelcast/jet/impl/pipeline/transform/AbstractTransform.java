/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.core.Vertex;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

public abstract class AbstractTransform implements Transform {

    @Nonnull
    private String name;

    @Nonnull
    private final List<Transform> upstream;

    private int localParallelism = Vertex.LOCAL_PARALLELISM_USE_DEFAULT;

    private final boolean[] upstreamRebalancingFlags;

    private final FunctionEx<?, ?>[] upstreamPartitionKeyFns;

    protected AbstractTransform(@Nonnull String name, @Nonnull List<Transform> upstream) {
        this.name = name;
        // Planner updates this list to fuse the stateless transforms:
        this.upstream = new ArrayList<>(upstream);
        this.upstreamRebalancingFlags = new boolean[upstream.size()];
        this.upstreamPartitionKeyFns = new FunctionEx[upstream.size()];
    }

    protected AbstractTransform(String name, @Nonnull Transform upstream) {
        this(name, singletonList(upstream));
    }

    @Nonnull @Override
    public List<Transform> upstream() {
        return upstream;
    }

    @Override
    public void setName(@Nonnull String name) {
        this.name = Objects.requireNonNull(name, "name");
    }

    @Nonnull @Override
    public String name() {
        return name;
    }

    @Override
    public void localParallelism(int localParallelism) {
        this.localParallelism = Vertex.checkLocalParallelism(localParallelism);
    }

    @Override
    public int localParallelism() {
        return localParallelism;
    }

    @Override
    public void setRebalanceInput(int ordinal, boolean value) {
        upstreamRebalancingFlags[ordinal] = value;
    }

    @Override
    public boolean shouldRebalanceInput(int ordinal) {
        return upstreamRebalancingFlags[ordinal];
    }

    @Override
    public void setRebalanceKeyForInput(int ordinal, FunctionEx<?, ?> keyFn) {
        upstreamPartitionKeyFns[ordinal] = keyFn;
    }

    @Override
    public FunctionEx<?, ?> partitionKeyFnForInput(int ordinal) {
        return upstreamPartitionKeyFns[ordinal];
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public long preferredWatermarkStride() {
        return 0;
    }

    protected final boolean shouldRebalanceAnyInput() {
        for (boolean b : upstreamRebalancingFlags) {
            if (b) {
                return true;
            }
        }
        return false;
    }
}
