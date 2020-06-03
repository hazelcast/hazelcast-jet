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

package com.hazelcast.jet.core;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.nCopies;
import static org.junit.Assert.fail;

public class HzSerializableProcessorSupplierTest extends SimpleTestInClusterSupport {

    private final DAG dag = new DAG();

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Test
    public void test_metaSupplier() {
        dag.newVertex("v", new MetaSupplier());
        instances()[1].newJob(dag).join();
    }

    @Test
    public void test_pSupplier() {
        dag.newVertex("v", new PSupplier());
        instances()[1].newJob(dag).join();
    }

    @Test
    public void test_simpleSupplier() {
        dag.newVertex("v", new SimpleSupplier());
        instances()[1].newJob(dag).join();
    }

    private static final class MetaSupplier implements ProcessorMetaSupplier, DataSerializable {
        @Nonnull @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new PSupplier();
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            fail("java serialization called");
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }

    private static final class PSupplier implements ProcessorSupplier, DataSerializable {
        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            return nCopies(count, Processors.noopP().get());
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            fail("java serialization called");
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }

    private static final class SimpleSupplier implements SupplierEx<Processor>, DataSerializable {
        @Override
        public Processor getEx() {
            return Processors.noopP().get();
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            fail("java serialization called");
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }
}
