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

package com.hazelcast.jet.sql.impl.connector;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.type.QueryDataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class Processors {

    private Processors() {
    }

    public static ProcessorSupplier rowProjector(
            String[] paths,
            QueryDataType[] types,
            SupplierEx<QueryTarget> targetSupplier,
            Expression<Boolean> predicate,
            List<Expression<?>> projection
    ) {
        return new Processors.RowProjectorProcessorSupplier(
                RowProjector.supplier(paths, types, targetSupplier, predicate, projection));
    }

    @SuppressFBWarnings(
            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized"
    )
    private static final class RowProjectorProcessorSupplier implements ProcessorSupplier, DataSerializable {

        private RowProjector.Supplier projectorSupplier;

        @SuppressWarnings("unused")
        private RowProjectorProcessorSupplier() {
        }

        RowProjectorProcessorSupplier(RowProjector.Supplier projectorSupplier) {
            this.projectorSupplier = projectorSupplier;
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            List<Processor> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                ResettableSingletonTraverser<Object[]> traverser = new ResettableSingletonTraverser<>();
                RowProjector projector = projectorSupplier.get();
                Processor processor = new TransformP<>(object -> {
                    traverser.accept(projector.project(object));
                    return traverser;
                });
                processors.add(processor);
            }
            return processors;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(projectorSupplier);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            projectorSupplier = in.readObject();
        }
    }
}
