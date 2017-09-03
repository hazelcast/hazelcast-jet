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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Implements the "collector" stage in a hash join transformation. This
 * stage collects the entire joined stream into a hashmap and then
 * broadcasts it to all local second-stage processors.
 */
public class HashJoinCollectP<K, E1, E1_OUT> extends AbstractProcessor {
    private final Map<K, List<E1_OUT>> map = new HashMap<>();
    @Nonnull private final Function<E1, K> keyF;
    @Nonnull private final Function<E1, E1_OUT> projectF;

    public HashJoinCollectP(@Nonnull Function<E1, K> keyF, @Nonnull Function<E1, E1_OUT> projectF) {
        this.keyF = keyF;
        this.projectF = projectF;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess0(@Nonnull Object item) throws Exception {
        E1 e1 = (E1) item;
        map.computeIfAbsent(keyF.apply(e1), k -> new ArrayList<>())
           .add(projectF.apply(e1));
        return true;
    }

    @Override
    public boolean complete() {
        return tryEmit(map);
    }
}
