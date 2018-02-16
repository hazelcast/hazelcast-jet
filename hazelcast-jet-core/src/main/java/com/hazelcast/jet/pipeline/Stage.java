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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.aggregate.AggregateOperation;

import javax.annotation.Nonnull;

/**
 * The basic element of a Jet {@link Pipeline pipeline}.
 * To build a pipeline, start with {@link Pipeline#drawFrom(BatchSource)} to
 * get the initial {@link BatchStage} and then use its methods to attach
 * further downstream stages. Terminate the pipeline by calling {@link
 * BatchStage#drainTo(Sink)}, which will attach a {@link SinkStage}.
 */
public interface Stage {
    /**
     * Returns the {@link Pipeline} this pipeline belongs to.
     */
    Pipeline getPipeline();

    @Nonnull
    Stage localParallelism(int localParallelism);

    /**
     * A <em>hint</em> to prefer DAG setup that uses less memory for this
     * stage. It is an opposite strategy to {@link #optimizeNetworkTraffic()}
     * (the default), see it for more details.
     */
    @Nonnull
    Stage optimizeMemory();

    /**
     * A <em>hint</em> to prefer DAG setup that transfers less data over the
     * network. This is the default strategy, the opposite strategy is {@link
     * #optimizeMemory()}.
     * <p>
     * Currently only aggregation stages consider this hint. It makes them to
     * choose two step aggregation. That is, first pre-aggregate items
     * locally, then send the partial results to target member processing the
     * key and combine them. This setup avoids serialization and network IO
     * costs by transferring only the pre-aggregated values. On the other hand,
     * each member might see all keys in the 1st stage so the memory usage can
     * be much higher.
     * <p>
     * Two step aggregation is not possible in this scenarios:<ul>
     *     <li>when using non-aligned windows (such as session windows)
     *     <li>if the aggregate operation doesn't support
     *     {@link AggregateOperation#combineFn() combine} primitive.
     * </ul>
     * pre deliver the item to the
     * member processing its partition and accumulate there. As a result
     * <p>
     * The other optimization strategy is {@link #optimizeNetworkTraffic()},
     * which is the default.
     */
    @Nonnull
    Stage optimizeNetworkTraffic();
}
