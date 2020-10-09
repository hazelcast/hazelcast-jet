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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;

public final class AggregateProcessors {

    private AggregateProcessors() {
    }

    public static ProcessorMetaSupplier combine(
            Address destinationMemberAddress,
            FunctionEx<Object, Object> partitionKeyFn,
            AggregateOperation<Aggregations, Object[]> aggregateOperation
    ) {
        return new CombineProcessorMetaSupplier(destinationMemberAddress, partitionKeyFn, aggregateOperation);
    }

    @SuppressFBWarnings(
            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized"
    )
    private static final class CombineProcessorMetaSupplier implements ProcessorMetaSupplier, DataSerializable {

        private transient Address destinationMemberAddress;
        private FunctionEx<Object, Object> partitionKeyFn;
        private AggregateOperation<Aggregations, Object[]> aggregationOperation;

        @SuppressWarnings("unused")
        private CombineProcessorMetaSupplier() {
        }

        private CombineProcessorMetaSupplier(
                Address destinationMemberAddress,
                FunctionEx<Object, Object> partitionKeyFn,
                AggregateOperation<Aggregations, Object[]> aggregationOperation
        ) {
            this.destinationMemberAddress = destinationMemberAddress;
            this.partitionKeyFn = partitionKeyFn;
            this.aggregationOperation = aggregationOperation;
        }

        @Nonnull
        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> address.equals(destinationMemberAddress)
                    ?
                    new ProcessorSupplier() {
                        @Nonnull
                        @Override
                        public Collection<? extends Processor> get(int count) {
                            assert count == 1 : "" + count;

                            return singletonList(new CombineP(partitionKeyFn, aggregationOperation));
                        }
                    }
                    :
                    count -> nCopies(count, new AbstractProcessor() {
                        @Override
                        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
                            throw new IllegalArgumentException("This vertex has a total parallelism of one"
                                    + " and expects input on a specific edge. Edge configuration must be adjusted"
                                    + " to make sure that only the expected node receives any input."
                                    + " Unexpected input received from ordinal " + ordinal + ": " + item
                            );
                        }

                        @Override
                        protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
                            // state might be broadcast to all instances - ignore it in the no-op instances
                        }
                    });
        }

        @Override
        public int preferredLocalParallelism() {
            return 1;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(destinationMemberAddress);
            out.writeObject(partitionKeyFn);
            out.writeObject(aggregationOperation);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            destinationMemberAddress = in.readObject();
            partitionKeyFn = in.readObject();
            aggregationOperation = in.readObject();
        }
    }

    private static final class CombineP extends AbstractProcessor {

        private final Map<Object, Aggregations> keyToAggregations;
        private final FunctionEx<Object, Object> partitionKeyFn;
        private final AggregateOperation<Aggregations, Object[]> aggregationOperation;

        private Traverser<Object[]> resultTraverser;

        private CombineP(
                FunctionEx<Object, Object> partitionKeyFn,
                AggregateOperation<Aggregations, Object[]> aggregationOperation
        ) {
            this.keyToAggregations = new HashMap<>();
            this.partitionKeyFn = partitionKeyFn;
            this.aggregationOperation = aggregationOperation;
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            Aggregations aggregations = keyToAggregations.computeIfAbsent(
                    partitionKeyFn.apply(item),
                    key -> aggregationOperation.createFn().get()
            );
            aggregationOperation.accumulateFn(ordinal).accept(aggregations, item);
            return true;
        }

        @Override
        public boolean complete() {
            if (resultTraverser == null) {
                resultTraverser = new ResultTraverser()
                        .map(aggregations -> aggregationOperation.finishFn().apply(aggregations));
            }
            return emitFromTraverser(resultTraverser);
        }

        private final class ResultTraverser implements Traverser<Aggregations> {

            private final Iterator<Aggregations> aggregations;

            private ResultTraverser() {
                this.aggregations = keyToAggregations.isEmpty()
                        ? new ArrayList<>(singletonList(aggregationOperation.createFn().get())).iterator()
                        : keyToAggregations.values().iterator();
            }

            @Override
            public Aggregations next() {
                if (!aggregations.hasNext()) {
                    return null;
                }
                try {
                    return aggregations.next();
                } finally {
                    aggregations.remove();
                }
            }
        }
    }
}
