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
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class SqlProcessors {

    private SqlProcessors() {
    }

    public static ProcessorSupplier projectEntrySupplier(
            UpsertTargetDescriptor keyDescriptor,
            UpsertTargetDescriptor valueDescriptor,
            List<TableField> fields
    ) {
        return new ProjectEntryProcessorSupplier(keyDescriptor, valueDescriptor, fields);
    }

    private static class ProjectEntryProcessorSupplier implements ProcessorSupplier, DataSerializable {

        private UpsertTargetDescriptor keyDescriptor;
        private UpsertTargetDescriptor valueDescriptor;

        private String[] names;
        private QueryDataType[] types;
        private boolean[] keys;

        private transient InternalSerializationService serializationService;

        @SuppressWarnings("unused")
        ProjectEntryProcessorSupplier() {
        }

        public ProjectEntryProcessorSupplier(
                UpsertTargetDescriptor keyDescriptor,
                UpsertTargetDescriptor valueDescriptor,
                List<TableField> fields
        ) {
            this.keyDescriptor = keyDescriptor;
            this.valueDescriptor = valueDescriptor;

            this.names = fields.stream().map(TableField::getName).toArray(String[]::new);
            this.types = fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);
            this.keys = toKeys(fields);
        }

        private static boolean[] toKeys(List<TableField> fields) {
            boolean[] result = new boolean[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                result[i] = ((MapTableField) fields.get(i)).getPath().isKey(); // TODO: get rid of casting ???
            }
            return result;
        }

        @Override
        public void init(@Nonnull Context context) {
            serializationService = ((ProcSupplierCtx) context).serializationService();
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            List<AbstractProcessor> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                ResettableSingletonTraverser<Object> traverser = new ResettableSingletonTraverser<>();
                EntryProjector projector = new EntryProjector(
                        keyDescriptor,
                        valueDescriptor,
                        serializationService,
                        names,
                        types,
                        keys
                );
                AbstractProcessor processor = new TransformP<Object[], Object>(row -> {
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
            out.writeUTFArray(names);
            out.writeObject(types);
            out.writeBooleanArray(keys);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            keyDescriptor = in.readObject();
            valueDescriptor = in.readObject();
            names = in.readUTFArray();
            types = in.readObject();
            keys = in.readBooleanArray();
        }
    }
}
