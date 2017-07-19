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
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.GroupAggregation;
import com.hazelcast.jet.pipeline.bag.BagsByTag;
import com.hazelcast.jet.pipeline.bag.Tag;
import com.hazelcast.jet.pipeline.bag.ThreeBags;
import com.hazelcast.jet.pipeline.bag.TwoBags;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * Javadoc pending.
 */
@SuppressWarnings("unchecked")
public class CoGroupP<K, B, A, R> extends AbstractProcessor {
    private final List<DistributedFunction<Object, ? extends K>> groupKeyFns;
    private final GroupAggregation<B, A, R> groupAggr;
    private final Class<B> bagsType;
    private final List<Tag> tags;

    private final List<Map<K, List<Object>>> groups;
    private final Set<K> seenKeys = new HashSet<>();

    public CoGroupP(List<DistributedFunction<Object, ? extends K>> groupKeyFns,
                    GroupAggregation<B, A, R> groupAggr,
                    Class<B> bagsType,
                    List<Tag> tags
    ) {
        this.groupKeyFns = groupKeyFns;
        this.groupAggr = groupAggr;
        this.bagsType = bagsType;
        this.groups = Stream.generate(HashMap<K, List<Object>>::new)
                            .limit(groupKeyFns.size())
                            .collect(toList());
        this.tags = tags;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        K groupKey = groupKeyFns.get(ordinal).apply(item);
        seenKeys.add(groupKey);
        groups.get(ordinal)
              .computeIfAbsent(groupKey, k -> new ArrayList<>())
              .add(item);
        return true;
    }

    @Override
    public boolean complete() {
        Function<K, B> keyToBagsF =
                bagsType == TwoBags.class ? k -> (B) new TwoBags(
                        lookupBag(0, k),
                        lookupBag(1, k))
              : bagsType == ThreeBags.class ? k -> (B) new ThreeBags(
                        lookupBag(0, k),
                        lookupBag(1, k),
                        lookupBag(2, k))
              : k -> {
                    BagsByTag bags = new BagsByTag();
                    for (int i = 0; i < tags.size(); i++) {
                        bags.put(tags.get(i), groups.get(i).getOrDefault(k, emptyList()));
                    }
                    return (B) bags;
                };
        return emitFromTraverser(
                traverseIterable(seenKeys)
                        .map(keyToBagsF)
                        .map(groupAggr.accumulateGroupF()));
    }

    private Iterable lookupBag(int ordinal, K key) {
        return groups.get(ordinal).getOrDefault(key, emptyList());
    }
}
