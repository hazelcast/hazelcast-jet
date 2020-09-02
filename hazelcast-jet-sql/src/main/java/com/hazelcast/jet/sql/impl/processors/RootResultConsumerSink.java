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

package com.hazelcast.jet.sql.impl.processors;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.sql.impl.JetQueryResultProducer;
import com.hazelcast.jet.sql.impl.JetSqlCoreBackendImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.JetSqlCoreBackend;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryResultProducer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.singleton;

public final class RootResultConsumerSink implements Processor {

    private final JetQueryResultProducer rootResultConsumer;

    private RootResultConsumerSink(QueryResultProducer rootResultConsumer) {
        this.rootResultConsumer = (JetQueryResultProducer) rootResultConsumer;
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        rootResultConsumer.consume(inbox);
    }

    @Override
    public boolean complete() {
        rootResultConsumer.done();
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    public static ProcessorMetaSupplier rootResultConsumerSink(Address initiatorAddress, QueryId queryId) {
        return new MetaSupplier(initiatorAddress, queryId);
    }

    private static final class MetaSupplier implements ProcessorMetaSupplier, DataSerializable {
        private Address initiatorAddress;
        private QueryId queryId;

        @SuppressWarnings("unused") // for deserialization
        private MetaSupplier() { }

        MetaSupplier(Address initiatorAddress, QueryId queryId) {
            this.initiatorAddress = initiatorAddress;
            this.queryId = queryId;
        }

        @Override
        public int preferredLocalParallelism() {
            return 1;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            if (context.localParallelism() != 1) {
                throw new Exception("Unexpected local parallelism: " + context.localParallelism());
            }
        }

        @Nonnull @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> initiatorAddress.equals(address)
                    ? new Supplier(queryId)
                    : ProcessorSupplier.of(NoInputProcessor::new);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(initiatorAddress);
            out.writeObject(queryId);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            initiatorAddress = in.readObject();
            queryId = in.readObject();
        }
    }

    private static final class Supplier implements ProcessorSupplier, DataSerializable {
        private QueryId queryId;

        private transient Map<QueryId, QueryResultProducer> resultConsumerRegistry;
        private transient QueryResultProducer rootResultConsumer;

        @SuppressWarnings("unused") // for deserialization
        private Supplier() { }

        private Supplier(QueryId queryId) {
            this.queryId = queryId;
        }

        @Override
        public void init(@Nonnull Context context) {
            HazelcastInstanceImpl hzInst = (HazelcastInstanceImpl) context.jetInstance().getHazelcastInstance();
            JetSqlCoreBackendImpl jetSqlCoreBackend = hzInst.node.nodeEngine.getService(JetSqlCoreBackend.SERVICE_NAME);
            resultConsumerRegistry = jetSqlCoreBackend.getResultConsumerRegistry();
            rootResultConsumer = resultConsumerRegistry.get(queryId);
            assert rootResultConsumer != null;
        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            assert count == 1;
            return singleton(new RootResultConsumerSink(rootResultConsumer));
        }

        @Override
        public void close(@Nullable Throwable error) {
            if (rootResultConsumer != null) {
                // make sure the consumer is closed. Most likely it already is done normally or already has an error
                if (error != null) {
                    rootResultConsumer.onError(QueryException.error(error.toString(), error));
                } else {
                    rootResultConsumer.onError(QueryException.error("Processor closed prematurely"));
                }
            }
            if (resultConsumerRegistry != null) {
                resultConsumerRegistry.remove(queryId);
            }
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(queryId);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            queryId = in.readObject();
        }
    }

    /**
     * A processor that throws if it receives any input.
     */
    private static class NoInputProcessor extends AbstractProcessor { }
}
