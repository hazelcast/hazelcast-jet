/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline.impl.transform;

import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.GroupAggregation;

import java.util.List;

/**
 * Javadoc pending.
 */
public class CoGroupTransform<K, B, A, R> implements JoinTransform {
    private final List<DistributedFunction<?, ? extends K>> groupKeyFns;
    private final GroupAggregation<B, A, R> groupAggr;
    private final Class bagsType;

    public CoGroupTransform(List<DistributedFunction<?, ? extends K>> groupKeyFns,
                            GroupAggregation<B, A, R> groupAggr,
                            Class bagsType
    ) {
        this.groupKeyFns = groupKeyFns;
        this.groupAggr = groupAggr;
        this.bagsType = bagsType;
    }

    public List<DistributedFunction<?, ? extends K>> groupKeyFns() {
        return groupKeyFns;
    }

    public GroupAggregation<B, A, R> groupAggr() {
        return groupAggr;
    }
}
