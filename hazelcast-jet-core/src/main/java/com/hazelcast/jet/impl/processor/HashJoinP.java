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
import com.hazelcast.jet.pipeline.JoinOn;
import com.hazelcast.jet.pipeline.bag.BagsByTag;
import com.hazelcast.jet.pipeline.bag.Tag;
import com.hazelcast.jet.pipeline.tuple.Tuple2;
import com.hazelcast.jet.pipeline.tuple.Tuple3;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

/**
 * Implements the hash-join transform.
 */
public class HashJoinP<E0> extends AbstractProcessor {

    private final List<JoinOn<?, E0, Object>> joinOns;
    private final List<Map<Object, List<Object>>> bagTables;
    private final List<Tag> tags;
    private boolean ordinal0consumed;

    public HashJoinP(@Nonnull List<JoinOn<?, E0, Object>> joinOns, @Nonnull List<Tag> tags) {
        // First list element is null so the indices in the list align with
        // the edge ordinals whose data they hold
        this.joinOns = new ArrayList<>(singletonList(null));
        this.joinOns.addAll(joinOns);
        this.bagTables = new ArrayList<>(singletonList(null));
        this.bagTables.addAll(Stream.generate(HashMap<Object, List<Object>>::new)
                                    .limit(joinOns.size())
                                    .collect(toList()));
        this.tags = new ArrayList<>(tags);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        assert !ordinal0consumed : "Edge 0 must have a lower priority than all other edges";
        Object joinKey = joinOns.get(ordinal).rightKeyFn().apply(item);
        bagTables.get(ordinal)
                 .computeIfAbsent(joinKey, k -> new ArrayList<>())
                 .add(item);
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess0(@Nonnull Object item) {
        E0 e0 = (E0) item;
        ordinal0consumed = true;
        if (tags.size() == 0) {
            return tryEmit(joinOns.size() == 2
                    ? new Tuple2<>(e0, lookupBag(1, e0))
                    : new Tuple3<>(e0, lookupBag(1, e0), lookupBag(2, e0)));
        }
        BagsByTag bags = new BagsByTag();
        for (int i = 1; i < joinOns.size(); i++) {
            bags.put(tags.get(i - 1), lookupBag(i, e0));
        }
        return tryEmit(new Tuple2<>(e0, bags));
    }

    private List<Object> lookupBag(int ordinal, E0 item) {
        return bagTables.get(ordinal).getOrDefault(leftKey(ordinal, item), emptyList());
    }

    private Object leftKey(int ordinal, E0 item) {
        return joinOns.get(ordinal).leftKeyFn().apply(item);
    }
}
