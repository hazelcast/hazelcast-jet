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
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class EntryProcessors {

    private EntryProcessors() {
    }

    public static ProcessorSupplier entryProjector(
            UpsertTargetDescriptor keyDescriptor,
            UpsertTargetDescriptor valueDescriptor,
            List<TableField> fields
    ) {
        return new EntryProjectorProcessorSupplier(keyDescriptor, valueDescriptor, fields);
    }

    @SuppressFBWarnings(
            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized"
    )
    private static class EntryProjectorProcessorSupplier implements ProcessorSupplier, DataSerializable {

        private UpsertTargetDescriptor keyDescriptor;
        private UpsertTargetDescriptor valueDescriptor;

        private QueryPath[] paths;
        private QueryDataType[] types;
        private Boolean[] hiddens;

        private transient InternalSerializationService serializationService;

        @SuppressWarnings("unused")
        EntryProjectorProcessorSupplier() {
        }

        EntryProjectorProcessorSupplier(
                UpsertTargetDescriptor keyDescriptor,
                UpsertTargetDescriptor valueDescriptor,
                List<TableField> fields
        ) {
            this.keyDescriptor = keyDescriptor;
            this.valueDescriptor = valueDescriptor;

            // TODO: get rid of casting ???
            this.paths = fields.stream().map(field -> ((MapTableField) field).getPath()).toArray(QueryPath[]::new);
            this.types = fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);
            this.hiddens = fields.stream().map(TableField::isHidden).toArray(Boolean[]::new);
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
                EntryProjector projector = new EntryProjector(
                        keyDescriptor.create(serializationService),
                        valueDescriptor.create(serializationService),
                        paths,
                        types,
                        hiddens
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
            out.writeObject(keyDescriptor);
            out.writeObject(valueDescriptor);
            out.writeInt(paths.length);
            for (QueryPath path : paths) {
                out.writeObject(path);
            }
            out.writeInt(types.length);
            for (QueryDataType type : types) {
                out.writeObject(type);
            }
            out.writeObject(hiddens);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            keyDescriptor = in.readObject();
            valueDescriptor = in.readObject();
            paths = new QueryPath[in.readInt()];
            for (int i = 0; i < paths.length; i++) {
                paths[i] = in.readObject();
            }
            types = new QueryDataType[in.readInt()];
            for (int i = 0; i < types.length; i++) {
                types[i] = in.readObject();
            }
            hiddens = in.readObject();
        }
    }
}
