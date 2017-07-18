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

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

/**
 * Javadoc pending.
 */
public class HashJoinP<K> extends AbstractProcessor {

    private final List<JoinOn<K, Object, Object>> joinOns;
    private final List<Map<Object, List<Object>>> lookupTables;

    public HashJoinP(List<JoinOn<K, Object, Object>> joinOns) {
        // First list element is null so the indices in the list align with
        // the edge ordinals whose data they hold
        this.joinOns = new ArrayList<>(singletonList(null));
        this.joinOns.addAll(joinOns);
        this.lookupTables = new ArrayList<>(singletonList(null));
        for (int i = 0; i < joinOns.size(); i++) {
            this.lookupTables.add(new HashMap<>());
        }
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        K joinKey = joinOns.get(ordinal).rightKeyFn().apply(item);
        lookupTables.get(ordinal)
                    .computeIfAbsent(joinKey, k -> new ArrayList<>())
                    .add(item);
        return true;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {

        return true;
    }
}
