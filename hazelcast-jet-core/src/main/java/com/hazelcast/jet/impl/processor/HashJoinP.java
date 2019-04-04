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

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.pipeline.transform.HashJoinTransform;
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

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Traversers.traverseIterable;
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

    private static final Object NONE = new Object();
    private static final Object[] NO_MATCH = new Object[]{NONE};

    private final List<Function<E0, Object>> keyFns;
    private final List<Map<Object, Object[]>> lookupTables;

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
        if (!tags.isEmpty()) {
            requireNonNull(mapToOutputBiFn,
                    "!tags.isEmpty(), but mapToOutputBiFn == null");
            this.flatMapper = itemsByTagFlatMapper(tags, mapToOutputBiFn);
            return;
        }
        if (keyFns.size() == 1) {
            BiFunction mapToOutput = requireNonNull(mapToOutputBiFn,
                    "tags.isEmpty() && keyFns.size() == 1, but mapToOutputBiFn == null");
            this.flatMapper = flatMapper(item ->
                    traverseArray(lookupValues(0, item))
                            .map(value -> mapToOutput.apply(item, matchedValue(value)))
            );
            return;
        }
        checkTrue(keyFns.size() == 2, "tags.isEmpty(), but keyFns.size() is neither 1 nor 2");
        TriFunction mapToOutput = requireNonNull(mapToOutputTriFn,
                "tags.isEmpty() && keyFns.size() == 2, but mapToOutputTriFn == null");
        this.flatMapper = flatMapper(object -> {
            Object[] valuesFor0 = lookupValues(0, object);
            Object[] valuesFor1 = lookupValues(1, object);
            return traverseArray(valuesFor0)
                    .flatMap(value0 -> traverseArray(valuesFor1)
                    .map(value1 -> mapToOutput.apply(object, matchedValue(value0), matchedValue(value1))));
        });
    }

    private FlatMapper<E0, Object> itemsByTagFlatMapper(@Nonnull List<Tag> tags, BiFunction mapToOutputBiFn) {
        assert mapToOutputBiFn != null : "mapToOutputBiFn required";
        Object[][] lookedUpValues = new Object[keyFns.size()][];
        List<ItemsByTag> values = new ArrayList<>();
        int[] indices = new int[keyFns.size()];
        return flatMapper(primaryItem -> {
            for (int i = 0; i < keyFns.size(); i++) {
                lookedUpValues[i] = lookupValues(i, primaryItem);
            }
            Arrays.fill(indices, 0);
            values.clear();
            while (indices[0] < lookedUpValues[0].length) {
                ItemsByTag val = new ItemsByTag();
                for (int j = 0; j < lookedUpValues.length; j++) {
                    val.put(tags.get(j), matchedValue(lookedUpValues[j][indices[j]]));
                }
                values.add(val);
                for (int j = indices.length - 1; j >= 0; j--) {
                    indices[j]++;
                    if (j == 0 || indices[j] < lookedUpValues[j].length) {
                        break;
                    }
                    indices[j] = 0;
                }
            }
            return traverseIterable(values)
                    .map(map -> mapToOutputBiFn.apply(primaryItem, map));
        });
    }

    private Object matchedValue(Object joinedObject) {
        return joinedObject == NONE ? null : joinedObject;
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
    private Object[] lookupValues(int index, E0 item) {
        Map<Object, Object[]> lookupTableForOrdinal = lookupTables.get(index);

        Object lookupTableKey = keyFns.get(index).apply(item);
        Object[] objects = lookupTableForOrdinal.get(lookupTableKey);

        return objects == null ? NO_MATCH : objects;
    }
}
