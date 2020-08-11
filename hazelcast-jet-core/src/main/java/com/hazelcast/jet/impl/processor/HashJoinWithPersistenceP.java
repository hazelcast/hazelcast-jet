/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.rocksdb.PrefixRocksMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.rocksdb.RocksIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static java.util.Objects.requireNonNull;

/**
 * A variant of {@link HashJoinP} that uses RocksDB state backend instead of in-memory maps to store it state.
 */
@SuppressWarnings("unchecked")
@SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
        justification = "https://github.com/spotbugs/spotbugs/issues/844")
public class HashJoinWithPersistenceP<E0> extends AbstractProcessor {

    private final List<Function<E0, Object>> keyFns;
    private final List<PrefixRocksMap> lookupTables;
    private final FlatMapper<E0, Object> flatMapper;
    //pool of native iterators, should be made in another way?
    private final List<RocksIterator> iterators;
    private boolean ordinal0Consumed;

    @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
            justification = "https://github.com/spotbugs/spotbugs/issues/844")
    public HashJoinWithPersistenceP(
            @Nonnull List<Function<E0, Object>> keyFns,
            @Nonnull List<Tag> tags,
            @Nullable BiFunction mapToOutputBiFn,
            @Nullable TriFunction mapToOutputTriFn,
            @Nullable BiFunctionEx<List<Tag>, Object[], ItemsByTag> tupleToItemsByTag
    ) {
        this.keyFns = keyFns;
        this.lookupTables = new ArrayList<>(Collections.nCopies(keyFns.size(), null));
        this.iterators = new ArrayList<>(Collections.nCopies(keyFns.size(), null));
        BiFunction<E0, Object[], Object> mapTupleToOutputFn;
        checkTrue(mapToOutputBiFn != null ^ mapToOutputTriFn != null,
                "Exactly one of mapToOutputBiFn and mapToOutputTriFn must be non-null");
        if (!tags.isEmpty()) {
            requireNonNull(mapToOutputBiFn, "mapToOutputBiFn required with tags");
            mapTupleToOutputFn = (item, tuple) -> {
                ItemsByTag res = tupleToItemsByTag.apply(tags, tuple);
                return res == null
                        ? null
                        : mapToOutputBiFn.apply(item, res);
            };
        } else if (keyFns.size() == 1) {
            BiFunction mapToOutput = requireNonNull(mapToOutputBiFn,
                    "tags.isEmpty() && keyFns.size() == 1, but mapToOutputBiFn == null");
            mapTupleToOutputFn = (item, tuple) -> mapToOutput.apply(item, tuple[0]);
        } else {
            checkTrue(keyFns.size() == 2, "tags.isEmpty(), but keyFns.size() is neither 1 nor 2");
            TriFunction mapToOutput = requireNonNull(mapToOutputTriFn,
                    "tags.isEmpty() && keyFns.size() == 2, but mapToOutputTriFn == null");
            mapTupleToOutputFn = (item, tuple) -> mapToOutput.apply(item, tuple[0], tuple[1]);
        }

        CombinationsTraverser traverser = new CombinationsTraverser(keyFns.size(), mapTupleToOutputFn);
        flatMapper = flatMapper(traverser::accept);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        assert !ordinal0Consumed : "Edge 0 must have a lower priority than all other edges";
        //for each map we keep one iterator for that map to limit the number of open iterators
        lookupTables.set(ordinal - 1, (PrefixRocksMap) item);
        iterators.set(ordinal - 1, ((PrefixRocksMap) item).prefixRocksIterator());
        return true;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        ordinal0Consumed = true;
        return flatMapper.tryProcess((E0) item);
    }

    private Object lookUpJoined(int index, E0 item) {
        PrefixRocksMap<Object, Object> lookupTableForOrdinal = lookupTables.get(index);
        RocksIterator rocksIterator = iterators.get(index);
        Object key = keyFns.get(index).apply(item);
        return getValues(lookupTableForOrdinal.getAllValues(rocksIterator, key));
    }

    @Nullable
    private Object getValues(@Nonnull Iterator iterator) {
        if (iterator.hasNext()) {
            Object x = iterator.next();
            if (iterator.hasNext()) {
                HashJoinArrayList result = new HashJoinArrayList();
                result.add(x);
                while (iterator.hasNext()) {
                    result.add(iterator.next());
                }
                return result;
            } else {
                return x;
            }
        }
        return null;
    }

    private class CombinationsTraverser<OUT> implements Traverser<OUT> {
        private final BiFunction<E0, Object[], OUT> mapTupleToOutputFn;
        private final Object[] lookedUpValues;
        private final int[] indices;
        private final int[] sizes;
        private final Object[] tuple;
        private E0 currentItem;

        CombinationsTraverser(int keyCount, BiFunction<E0, Object[], OUT> mapTupleToOutputFn) {
            this.mapTupleToOutputFn = mapTupleToOutputFn;

            lookedUpValues = new Object[keyCount];
            indices = new int[keyCount];
            sizes = new int[keyCount];
            tuple = new Object[keyCount];
        }

        /**
         * Accept the next item to traverse. The previous item must be fully
         * traversed.
         */
        CombinationsTraverser<OUT> accept(E0 item) {
            assert currentItem == null : "currentItem not null";
            // look up matching values for each joined table
            for (int i = 0; i < lookedUpValues.length; i++) {
                lookedUpValues[i] = lookUpJoined(i, item);
                sizes[i] = lookedUpValues[i] instanceof HashJoinArrayList
                        ? ((HashJoinArrayList) lookedUpValues[i]).size() : 1;
            }
            Arrays.fill(indices, 0);
            currentItem = item;
            return this;
        }

        @Override
        public OUT next() {
            while (indices[0] < sizes[0]) {
                // populate the tuple and create the result object
                for (int j = 0; j < lookedUpValues.length; j++) {
                    tuple[j] = sizes[j] == 1 ? lookedUpValues[j]
                            : ((HashJoinArrayList) lookedUpValues[j]).get(indices[j]);
                }
                OUT result = mapTupleToOutputFn.apply(currentItem, tuple);

                // advance indices to the next combination
                for (int j = indices.length - 1; j >= 0; j--) {
                    indices[j]++;
                    if (j == 0 || indices[j] < sizes[j]) {
                        break;
                    }
                    indices[j] = 0;
                }

                if (result != null) {
                    return result;
                }
            }

            currentItem = null;
            return null;
        }
    }

    // We need a custom ArrayList subclass because the user's V type could be
    // ArrayList and then the logic that relies on instanceof would break
    static final class HashJoinArrayList extends ArrayList<Object> {
        HashJoinArrayList() {
            super(2);
        }
    }
}
