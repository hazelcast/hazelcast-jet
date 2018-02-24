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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.pipeline.BatchStage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Implements the {@link com.hazelcast.jet.impl.pipeline.transform.HashJoinTransform
 * hash-join transform}. On all edges except 0 it will receive a single
 * item &mdash; the lookup table for that edge (a {@code Map}) and then it
 * will process edge 0 by joining to each item the data from lookup tables.
 * It will extract a separate key for each of the lookup tables using the
 * functions supplied in the {@code keyFns} argument. Element 0 in that list
 * corresponds to the lookup table received at ordinal 1 and so on.
 * <p>
 * The {@code tags} is used to populate the output items. It can be {@code
 * null}, in which case {@code keyFns} must have either one or two elements,
 * corresponding to the two supported special cases in {@link BatchStage}.
 * <p>
 * After looking up all the joined items the processor calls the supplied
 * {@code mapToOutput*Fn} to get the final output item. It uses {@code
 * mapToOutputBiFn} both for the single-arity case ({@code tags == null &&
 * keyFns.size() == 1}) and the variable-arity case ({@code tags != null}).
 * In the latter case the function must expect {@code ItemsByTag} as the
 * second argument.
 * <p>
 * Note that internally the processor stores the lists with a {@code null}
 * element prepended to remove the mismatch between list index and ordinal.
 */
public class HashJoinP<E0> extends AbstractProcessor {

    private final List<Function<E0, Object>> keyFns;
    private final List<Map<Object, Object>> lookupTables;
    private final List<Tag> tags;
    private final BiFunction mapToOutputBiFn;
    private final TriFunction mapToOutputTriFn;
    private boolean ordinal0consumed;

    public HashJoinP(
            @Nonnull List<Function<E0, Object>> keyFns,
            @Nonnull List<Tag> tags,
            @Nullable BiFunction mapToOutputBiFn,
            @Nullable TriFunction mapToOutputTriFn
    ) {
        this.keyFns = prependNull(keyFns);
        this.lookupTables = prependNull(Collections.nCopies(keyFns.size(), null));
        this.tags = tags.isEmpty() ? emptyList() : prependNull(tags);
        this.mapToOutputBiFn = mapToOutputBiFn;
        this.mapToOutputTriFn = mapToOutputTriFn;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        assert !ordinal0consumed : "Edge 0 must have a lower priority than all other edges";
        lookupTables.set(ordinal, (Map) item);
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess0(@Nonnull Object item) {
        E0 e0 = (E0) item;
        ordinal0consumed = true;
        if (tags.isEmpty()) {
            return tryEmit(keyFns.size() == 2
                    ? mapToOutputBiFn.apply(e0, lookupJoined(1, e0))
                    : mapToOutputTriFn.apply(e0, lookupJoined(1, e0), lookupJoined(2, e0)));
        }
        ItemsByTag map = new ItemsByTag();
        for (int i = 1; i < keyFns.size(); i++) {
            map.put(tags.get(i), lookupJoined(i, e0));
        }
        return tryEmit(mapToOutputBiFn.apply(e0, map));
    }

    @Nullable
    private Object lookupJoined(int ordinal, E0 item) {
        return lookupTables.get(ordinal).get(keyFns.get(ordinal).apply(item));
    }

    private static <E> List<E> prependNull(List<E> in) {
        List<E> result = new ArrayList<>(singletonList(null));
        result.addAll(in);
        return result;
    }
}
