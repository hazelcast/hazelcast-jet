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
import java.util.Map.Entry;
import java.util.function.Function;

import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;

public final class AggregateProcessors {

    private AggregateProcessors() {
    }

    public static ProcessorMetaSupplier combineByKeyP(
            Address localMemberAddress,
            AggregateOperation<Aggregator, Object[]> aggregateOperation
    ) {
        return new CombineProcessorMetaSupplier(
                localMemberAddress,
                aggregateOperation.withCombiningAccumulateFn(Entry<ObjectArray, Aggregator>::getValue)
        );
    }

    @SuppressFBWarnings(
            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized"
    )
    private static final class CombineProcessorMetaSupplier implements ProcessorMetaSupplier, DataSerializable {

        private Address localMemberAddress;
        private AggregateOperation<Aggregator, Object[]> aggregateOperation;

        @SuppressWarnings("unused")
        private CombineProcessorMetaSupplier() {
        }

        private CombineProcessorMetaSupplier(
                Address localMemberAddress,
                AggregateOperation<Aggregator, Object[]> aggregateOperation
        ) {
            this.localMemberAddress = localMemberAddress;
            this.aggregateOperation = aggregateOperation;
        }

        @Nonnull
        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> address.equals(localMemberAddress)
                    ? new CombineProcessorSupplier(aggregateOperation)
                    : count -> nCopies(count, new AbstractProcessor() {
                @Override
                protected boolean tryProcess(int ordinal, @Nonnull Object item) {
                    throw new IllegalStateException(
                            "This vertex has a total parallelism of one and as such only"
                                    + " expects input on a specific node. Edge configuration must be adjusted"
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
            out.writeObject(localMemberAddress);
            out.writeObject(aggregateOperation);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            localMemberAddress = in.readObject();
            aggregateOperation = in.readObject();
        }
    }

    @SuppressFBWarnings(
            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized"
    )
    private static final class CombineProcessorSupplier implements ProcessorSupplier, DataSerializable {

        private AggregateOperation<Aggregator, Object[]> aggregateOperation;

        @SuppressWarnings("unused")
        private CombineProcessorSupplier() {
        }

        CombineProcessorSupplier(AggregateOperation<Aggregator, Object[]> aggregateOperation) {
            this.aggregateOperation = aggregateOperation;
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            assert count == 1 : "count = " + count;

            return singletonList(new CombineP(aggregateOperation));
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(aggregateOperation);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            aggregateOperation = in.readObject();
        }
    }

    private static class CombineP extends AbstractProcessor {

        private final Map<ObjectArray, Aggregator> keyToAccumulator;
        private final AggregateOperation<Aggregator, Object[]> aggregateOperation;

        private Traverser<Object[]> resultTraverser;

        CombineP(
                AggregateOperation<Aggregator, Object[]> aggregateOperation
        ) {
            this.keyToAccumulator = new HashMap<>();
            this.aggregateOperation = aggregateOperation;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            Entry<ObjectArray, Object[]> entry = (Entry<ObjectArray, Object[]>) item;

            ObjectArray key = entry.getKey();
            Aggregator accumulator = keyToAccumulator.computeIfAbsent(key, k -> aggregateOperation.createFn().get());
            aggregateOperation.accumulateFn(ordinal).accept(accumulator, item);
            return true;
        }

        @Override
        public boolean complete() {
            if (resultTraverser == null) {
                resultTraverser = new ResultTraverser()
                        .map(accumulator -> aggregateOperation.finishFn().apply(accumulator));
            }
            return emitFromTraverser(resultTraverser);
        }

        private class ResultTraverser implements Traverser<Aggregator> {

            private final Iterator<Aggregator> accumulators;

            private ResultTraverser() {
                this.accumulators = keyToAccumulator.isEmpty()
                        ? new ArrayList<>(singletonList(aggregateOperation.createFn().get())).iterator()
                        : keyToAccumulator.values().iterator();
            }

            @Override
            public Aggregator next() {
                if (!accumulators.hasNext()) {
                    return null;
                }
                try {
                    return accumulators.next();
                } finally {
                    accumulators.remove();
                }
            }
        }
    }
}
