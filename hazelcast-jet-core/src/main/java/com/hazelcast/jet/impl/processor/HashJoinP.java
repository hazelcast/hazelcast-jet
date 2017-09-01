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
import com.hazelcast.jet.pipeline.bag.BagsByTag;
import com.hazelcast.jet.pipeline.bag.Tag;
import com.hazelcast.jet.pipeline.tuple.Tuple2;
import com.hazelcast.jet.pipeline.tuple.Tuple3;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

/**
 * Implements the {@link com.hazelcast.jet.pipeline.impl.transform.HashJoinTransform
 * hash-join transform}.
 */
public class HashJoinP<E0> extends AbstractProcessor {

    private final List<Function<E0, Object>> keyFns;
    private final List<Map<Object, List<Object>>> bagTables;
    private final List<Tag> tags;
    private boolean ordinal0consumed;

    /**
     * Constructs a HashJoin processor. On all edges except 0 it will receive
     * a single item &mdash; the lookup table for that edge (a {@code Map}) and
     * then it will process edge 0 by joining to each item the data from lookup
     * tables. It will extract a separate key for each of the lookup tables
     * using the functions supplied in the {@code keyFns} argument. Element 0 in
     * that list corresponds to the lookup table received at ordinal 1 and so
     * on.
     * <p>
     * The {@code tags} is used to populate the output items. It can be {@code
     * null}, in which case {@code keyFns} must have either one or two elements,
     * corresponding to the two supported special cases in
     * {@link com.hazelcast.jet.pipeline.ComputeStage}.
     * <p>
     * Note that internally the processor stores the lists with a {@code null}
     * element prepended to remove the mismatch between list index and ordinal.
     */
    public HashJoinP(@Nonnull List<Function<E0, Object>> keyFns, @Nonnull List<Tag> tags) {
        // First list element is null so the indices in the list align with
        // the edge ordinals whose data they hold
        this.keyFns = new ArrayList<>(singletonList(null));
        this.keyFns.addAll(keyFns);
        this.bagTables = Stream.generate(() -> (Map<Object, List<Object>>) null)
                               .limit(this.keyFns.size())
                               .collect(toList());
        if (tags.isEmpty()) {
            this.tags = emptyList();
        } else {
            this.tags = new ArrayList<>(singletonList(null));
            this.tags.addAll(tags);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        assert !ordinal0consumed : "Edge 0 must have a lower priority than all other edges";
        bagTables.set(ordinal, (Map) item);
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess0(@Nonnull Object item) {
        E0 e0 = (E0) item;
        ordinal0consumed = true;
        if (tags.size() == 0) {
            return tryEmit(keyFns.size() == 2
                    ? new Tuple2<>(e0, lookupBag(1, e0))
                    : new Tuple3<>(e0, lookupBag(1, e0), lookupBag(2, e0)));
        }
        BagsByTag bags = new BagsByTag();
        for (int i = 1; i < keyFns.size(); i++) {
            bags.put(tags.get(i), lookupBag(i, e0));
        }
        return tryEmit(new Tuple2<>(e0, bags));
    }

    private List<Object> lookupBag(int ordinal, E0 item) {
        return bagTables.get(ordinal).getOrDefault(leftKey(ordinal, item), emptyList());
    }

    private Object leftKey(int ordinal, E0 item) {
        return keyFns.get(ordinal).apply(item);
    }
}
