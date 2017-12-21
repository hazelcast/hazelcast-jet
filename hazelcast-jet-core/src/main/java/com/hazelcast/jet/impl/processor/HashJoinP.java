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

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Implements the {@link com.hazelcast.jet.impl.transform.HashJoinTransform
 * hash-join transform}.
 */
public class HashJoinP<T0> extends AbstractProcessor {

    private final List<Function<T0, Object>> keyFns;
    private final List<Map<Object, Object>> lookupTables;
    private final List<Tag> tags;
    private boolean ordinal0consumed;

    /**
     * Constructs a HashJoin processor. On all edges except 0 it will receive
     * a single item &mdash; the lookup table for that edge (a {@code Map}) and
     * then it will process edge 0 by joining to each item the data from lookup
     * tables. It will extract a separate key for each of the lookup tables
     * using the functions supplied in the {@code keyFs} argument. Element 0 in
     * that list corresponds to the lookup table received at ordinal 1 and so
     * on.
     * <p>
     * The {@code tags} is used to populate the output items. It can be {@code
     * null}, in which case {@code keyFs} must have either one or two elements,
     * corresponding to the two supported special cases in
     * {@link com.hazelcast.jet.ComputeStage}.
     * <p>
     * Note that internally the processor stores the lists with a {@code null}
     * element prepended to remove the mismatch between list index and ordinal.
     */
    public HashJoinP(
            @Nonnull List<Function<T0, Object>> keyFns,
            @Nonnull List<Tag> tags
    ) {
        this.keyFns = prependNull(keyFns);
        this.lookupTables = prependNull(Collections.nCopies(keyFns.size(), null));
        this.tags = tags.isEmpty() ? emptyList() : prependNull(tags);
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
        T0 t0 = (T0) item;
        ordinal0consumed = true;
        if (tags.isEmpty()) {
            return tryEmit(keyFns.size() == 2
                    ? tuple2(t0, lookupJoined(1, t0))
                    : tuple3(t0, lookupJoined(1, t0), lookupJoined(2, t0)));
        }
        ItemsByTag ibt = new ItemsByTag();
        for (int i = 1; i < keyFns.size(); i++) {
            ibt.put(tags.get(i), lookupJoined(i, t0));
        }
        return tryEmit(tuple2(t0, ibt));
    }

    @Nullable
    private Object lookupJoined(int ordinal, T0 item) {
        return lookupTables.get(ordinal).get(keyFns.get(ordinal).apply(item));
    }

    private static <E> List<E> prependNull(List<E> in) {
        List<E> result = new ArrayList<>(singletonList(null));
        result.addAll(in);
        return result;
    }
}
