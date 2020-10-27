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
import com.hazelcast.jet.impl.pipeline.PipelineImpl.Context;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.jet.core.Vertex.checkLocalParallelism;
import static java.lang.Math.min;
import static java.util.Collections.singletonList;

public abstract class AbstractTransform implements Transform {

    @Nonnull
    private String name;

    @Nonnull
    private final List<Transform> upstream;

    private int localParallelism = Vertex.LOCAL_PARALLELISM_USE_DEFAULT;

    private int determinedLocalParallelism = Vertex.LOCAL_PARALLELISM_USE_DEFAULT;

    private final boolean[] upstreamRebalancingFlags;

    private boolean preserveEventOrder;

    private boolean orderSensitive;

    private boolean sequencer;

    private final FunctionEx<?, ?>[] upstreamPartitionKeyFns;

    protected AbstractTransform(@Nonnull String name, @Nonnull List<Transform> upstream) {
        this.name = name;
        // Planner updates this list to fuse the stateless transforms:
        this.upstream = new ArrayList<>(upstream);
        this.upstreamRebalancingFlags = new boolean[upstream.size()];
        this.preserveEventOrder = false;
        this.orderSensitive = false;
        this.sequencer = false;
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
        this.localParallelism = checkLocalParallelism(localParallelism);
    }

    @Override
    public int localParallelism() {
        return localParallelism;
    }

    @Override
    public void determinedLocalParallelism(int determinedLocalParallelism) {
        this.determinedLocalParallelism = checkLocalParallelism(determinedLocalParallelism);
    }

    @Override
    public int determinedLocalParallelism() {
        return determinedLocalParallelism;
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
    public void setPartitionKeyFnForInput(int ordinal, FunctionEx<?, ?> keyFn) {
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

    @Override
    public boolean shouldPreserveEventOrder() {
        return preserveEventOrder;
    }

    @Override
    public boolean isOrderSensitive() {
        return orderSensitive;
    }

    @Override
    public void setOrderSensitive(boolean value) {
        orderSensitive = value;
    }

    @Override
    public boolean isSequencer() {
        return sequencer;
    }

    @Override
    public void setSequencer(boolean value) {
        sequencer = value;
    }

    @Override
    public void setPreserveEventOrder(boolean value) {
        preserveEventOrder = value;
    }

    protected final boolean shouldRebalanceAnyInput() {
        for (boolean b : upstreamRebalancingFlags) {
            if (b) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determines the local parallelism value for the transform by looking at
     * local parallelism of its upstream transform its local parallelism, preferred local parallelism, and the default
     * local parallelism provided in Pipeline.Context object.
     * <p>
     * If none of them is set, returns the default local parallelism
     * provided in PipelineImpl.Context object.
     */
    protected void determineLocalParallelism(int preferredLocalParallelism, Context context,
                                             boolean shouldMatchUpstreamParallelism) {

        int defaultParallelism = context.defaultLocalParallelism();
        int upstreamParallelism = -1;
        // Here I assume that this upstreamLocalParallelism is already
        // ineffective in the LP determination of the transforms with
        // multiple upstream transforms. Or if it is effective, then
        // the LPs of upstreams should be equal. So, only get the first
        // upstream LP value as upstreamParallelism.
        if (!upstream().isEmpty()) {
            upstreamParallelism = upstream.get(0).determinedLocalParallelism();
        }

        if (shouldMatchUpstreamParallelism && upstreamParallelism != Vertex.LOCAL_PARALLELISM_USE_DEFAULT
                && shouldPreserveEventOrder()) {
            determinedLocalParallelism(upstreamParallelism);
            return;
        }

        if (localParallelism() == Vertex.LOCAL_PARALLELISM_USE_DEFAULT) {
            if (preferredLocalParallelism == Vertex.LOCAL_PARALLELISM_USE_DEFAULT) {
                determinedLocalParallelism(defaultParallelism);
            } else {
                if (defaultParallelism == Vertex.LOCAL_PARALLELISM_USE_DEFAULT) {
                    determinedLocalParallelism(preferredLocalParallelism);
                } else {
                    determinedLocalParallelism(min(preferredLocalParallelism, defaultParallelism));
                }
            }
        } else {
            determinedLocalParallelism(localParallelism());
        }
    }
}
