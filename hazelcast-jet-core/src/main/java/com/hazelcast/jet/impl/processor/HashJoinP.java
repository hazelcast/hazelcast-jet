/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.pipeline.transform.HashJoinTransform;
import com.hazelcast.jet.impl.processor.HashJoinCollectP.HashJoinArrayList;
import com.hazelcast.jet.pipeline.BatchStage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Objects.requireNonNull;

/**
 * Implements the {@linkplain HashJoinTransform hash-join transform}. On
 * all edges except 0 it receives a single item &mdash; the lookup table
 * for that edge (a {@code Map}) and then it processes edge 0 by joining
 * to each item the data from the lookup tables.
 * <p>
 * It extracts a separate key for each of the lookup tables using the
 * functions supplied in the {@code keyFns} argument. Element 0 in that
 * list corresponds to the lookup table received at ordinal 1 and so on.
 * <p>
 * It uses the {@code tags} list to populate the output items. It can be
 * {@code null}, in which case {@code keyFns} must have either one or two
 * elements, corresponding to the two supported special cases in {@link
 * BatchStage} (hash-joining with one or two enriching streams).
 * <p>
 * After looking up all the joined items the processor calls the supplied
 * {@code mapToOutput*Fn} to get the final output item. It uses {@code
 * mapToOutputBiFn} both for the single-arity case ({@code tags == null &&
 * keyFns.size() == 1}) and the variable-arity case ({@code tags != null}).
 * In the latter case the function must expect {@code ItemsByTag} as the
 * second argument. It uses {@code mapToOutputTriFn} for the two-arity
 * case ({@code tags == null && keyFns.size() == 2}).
 */
@SuppressWarnings("unchecked")
public class HashJoinP<E0> extends AbstractProcessor {

    private final List<Function<E0, Object>> keyFns;
    private final List<Map<Object, Object>> lookupTables;
    private final FlatMapper<E0, Object> flatMapper;

    private boolean ordinal0consumed;

    public HashJoinP(
            @Nonnull List<Function<E0, Object>> keyFns,
            @Nonnull List<Tag> tags,
            @Nullable BiFunction mapToOutputBiFn,
            @Nullable TriFunction mapToOutputTriFn
    ) {
        this.keyFns = keyFns;
        this.lookupTables = new ArrayList<>(Collections.nCopies(keyFns.size(), null));
        BiFunction<E0, Object[], Object> outputMapper;
        if (!tags.isEmpty()) {
            assert mapToOutputBiFn != null : "mapToOutputBiFn required with tags";
            outputMapper = (item, tuple) -> {
                ItemsByTag res = new ItemsByTag();
                for (int i = 0; i < tags.size(); i++) {
                    res.put(tags.get(i), tuple[i]);
                }
                return mapToOutputBiFn.apply(item, res);
            };
        } else if (keyFns.size() == 1) {
            BiFunction mapToOutput = requireNonNull(mapToOutputBiFn,
                    "tags.isEmpty() && keyFns.size() == 1, but mapToOutputBiFn == null");
            outputMapper = (item, tuple) -> mapToOutput.apply(item, tuple[0]);
        } else {
            checkTrue(keyFns.size() == 2, "tags.isEmpty(), but keyFns.size() is neither 1 nor 2");
            TriFunction mapToOutput = requireNonNull(mapToOutputTriFn,
                    "tags.isEmpty() && keyFns.size() == 2, but mapToOutputTriFn == null");
            outputMapper = (item, tuple) -> mapToOutput.apply(item, tuple[0], tuple[1]);
        }

        TraverseCombinations traverser = new TraverseCombinations(keyFns.size(), outputMapper);
        flatMapper = flatMapper(traverser::accept);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        assert !ordinal0consumed : "Edge 0 must have a lower priority than all other edges";
        lookupTables.set(ordinal - 1, (Map) item);
        return true;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        E0 e0 = (E0) item;
        ordinal0consumed = true;
        return flatMapper.tryProcess(e0);
    }

    @Nonnull
    private Object lookupJoined(int index, E0 item) {
        Map<Object, Object> lookupTableForOrdinal = lookupTables.get(index);
        Object key = keyFns.get(index).apply(item);
        return lookupTableForOrdinal.get(key);
    }

    private class TraverseCombinations<OUT> implements Traverser<OUT> {
        private final BiFunction<E0, Object[], OUT> outputMapper;
        private final Object[] lookedUpValues;
        private final int[] indices;
        private final int[] sizes;
        private final Object[] tuple;
        private E0 currentItem;

        TraverseCombinations(int keyCount, BiFunction<E0, Object[], OUT> outputMapper) {
            this.outputMapper = outputMapper;

            lookedUpValues = new Object[keyCount];
            indices = new int[keyCount];
            sizes = new int[keyCount];
            tuple = new Object[keyCount];
        }

        TraverseCombinations<OUT> accept(E0 item) {
            assert Arrays.stream(indices).allMatch(i -> i == 0) : "indices not all zero: " + Arrays.toString(indices);
            for (int i = 0; i < lookedUpValues.length; i++) {
                lookedUpValues[i] = lookupJoined(i, item);
                sizes[i] = lookedUpValues[i] instanceof HashJoinArrayList
                        ? ((HashJoinArrayList) lookedUpValues[i]).size() : 1;
            }
            currentItem = item;
            return this;
        }

        @Override
        public OUT next() {
            while (indices[0] < sizes[0]) {
                for (int j = 0; j < lookedUpValues.length; j++) {
                    tuple[j] = sizes[j] == 1 ? lookedUpValues[j]
                            : ((HashJoinArrayList) lookedUpValues[j]).get(indices[j]);
                }
                for (int j = indices.length - 1; j >= 0; j--) {
                    indices[j]++;
                    if (j == 0 || indices[j] < sizes[j]) {
                        break;
                    }
                    indices[j] = 0;
                }
                OUT res = outputMapper.apply(currentItem, tuple);
                if (res != null) {
                    return res;
                }
            }
            Arrays.fill(indices, 0);
            return null;
        }
    }
}
