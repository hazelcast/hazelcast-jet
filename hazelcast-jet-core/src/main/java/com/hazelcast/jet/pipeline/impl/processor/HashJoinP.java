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

package com.hazelcast.jet.pipeline.impl.processor;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.pipeline.JoinOn;
import com.hazelcast.jet.pipeline.bag.BagsByTag;
import com.hazelcast.jet.pipeline.bag.Tag;
import com.hazelcast.jet.pipeline.bag.ThreeBags;
import com.hazelcast.jet.pipeline.bag.TwoBags;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

/**
 * Javadoc pending.
 */
public class HashJoinP<K> extends AbstractProcessor {

    private final List<JoinOn<K, Object, Object>> joinOns;
    private final List<Map<Object, List<Object>>> lookupTables;
    private final Class bagsType;
    private final List<Tag> tags;
    private boolean ordinal0consumed;

    public HashJoinP(List<JoinOn<K, Object, Object>> joinOns, Class bagsType, List<Tag> tags) {
        if (bagsType == BagsByTag.class) {
            checkTrue(joinOns.size() == tags.size(), "Number of joinOns must match the number of bag tags");
        }
        // First list element is null so the indices in the list align with
        // the edge ordinals whose data they hold
        this.joinOns = new ArrayList<>(singletonList(null));
        this.joinOns.addAll(joinOns);
        this.lookupTables = new ArrayList<>(singletonList(null));
        this.lookupTables.addAll(Stream.generate(HashMap<Object, List<Object>>::new)
                                       .limit(joinOns.size())
                                       .collect(toList()));
        this.bagsType = bagsType;
        this.tags = new ArrayList<>(tags);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        assert !ordinal0consumed : "All edges with ordinal > 0 must have a higher priority than edge with ordinal 0";
        K joinKey = joinOns.get(ordinal).rightKeyFn().apply(item);
        lookupTables.get(ordinal)
                    .computeIfAbsent(joinKey, k -> new ArrayList<>())
                    .add(item);
        return true;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        ordinal0consumed = true;
        if (bagsType == TwoBags.class) {
            return tryEmit(new TwoBags<>(
                    lookupBag(1, item),
                    lookupBag(2, item)));
        }
        if (bagsType == ThreeBags.class) {
            return tryEmit(new ThreeBags<>(
                    lookupBag(1, item),
                    lookupBag(2, item),
                    lookupBag(3, item)));
        }
        BagsByTag bags = new BagsByTag();
        for (int i = 1; i < joinOns.size(); i++) {
            bags.put(tags.get(i), lookupBag(i, item));
        }
        return tryEmit(bags);
    }

    private K leftKey(int ordinal, Object item) {
        return joinOns.get(ordinal).leftKeyFn().apply(item);
    }

    private List<Object> lookupBag(int ordinal, Object item) {
        return lookupTables.get(ordinal).getOrDefault(leftKey(ordinal, item), emptyList());
    }
}
