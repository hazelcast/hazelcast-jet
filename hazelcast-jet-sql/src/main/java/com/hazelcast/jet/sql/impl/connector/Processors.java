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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class Processors {

    private Processors() {
    }

    public static ProcessorSupplier projector(
            UpsertTargetDescriptor descriptor,
            String[] paths,
            QueryDataType[] types
    ) {
        return new ProjectorProcessorSupplier(descriptor, paths, types);
    }

    private static class ProjectorProcessorSupplier implements ProcessorSupplier, DataSerializable {

        private UpsertTargetDescriptor descriptor;

        private String[] paths;
        private QueryDataType[] types;

        private transient InternalSerializationService serializationService;

        @SuppressWarnings("unused")
        ProjectorProcessorSupplier() {
        }

        ProjectorProcessorSupplier(
                UpsertTargetDescriptor descriptor,
                String[] paths,
                QueryDataType[] types
        ) {
            this.descriptor = descriptor;

            this.paths = paths;
            this.types = types;
        }

        @Override
        public void init(@Nonnull Context context) {
            serializationService = ((ProcSupplierCtx) context).serializationService();
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            List<Processor> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                ResettableSingletonTraverser<Object> traverser = new ResettableSingletonTraverser<>();
                Projector projector = new Projector(
                        descriptor.create(serializationService),
                        paths,
                        types
                );
                Processor processor = new TransformP<Object[], Object>(row -> {
                    traverser.accept(projector.project(row));
                    return traverser;
                });
                processors.add(processor);
            }
            return processors;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(descriptor);
            out.writeUTFArray(paths);
            out.writeInt(types.length);
            for (QueryDataType type : types) {
                out.writeObject(type);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            descriptor = in.readObject();
            paths = in.readUTFArray();
            types = new QueryDataType[in.readInt()];
            for (int i = 0; i < types.length; i++) {
                types[i] = in.readObject();
            }
        }
    }
}
