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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.portable.FieldDefinitionImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;

import java.io.IOException;

public class PortableUpsertTargetDescriptor implements UpsertTargetDescriptor {

    private ClassDefinition classDefinition;

    @SuppressWarnings("unused")
    private PortableUpsertTargetDescriptor() {
    }

    public PortableUpsertTargetDescriptor(ClassDefinition classDefinition) {
        this.classDefinition = classDefinition;
    }

    @Override
    public UpsertTarget create(InternalSerializationService serializationService) {
        return new PortableUpsertTarget(classDefinition);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(classDefinition.getFactoryId());
        out.writeInt(classDefinition.getClassId());
        out.writeInt(classDefinition.getVersion());
        out.writeInt(classDefinition.getFieldCount());
        for (int i = 0; i < classDefinition.getFieldCount(); i++) {
            FieldDefinition fieldDefinition = classDefinition.getField(i);
            out.writeInt(fieldDefinition.getIndex());
            out.writeString(fieldDefinition.getName());
            out.writeByte(fieldDefinition.getType().getId());
            out.writeInt(fieldDefinition.getFactoryId());
            out.writeInt(fieldDefinition.getClassId());
            out.writeInt(fieldDefinition.getVersion());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int factoryId = in.readInt();
        int classId = in.readInt();
        int classVersion = in.readInt();
        ClassDefinitionBuilder classDefinition = new ClassDefinitionBuilder(factoryId, classId, classVersion);

        int fieldCount = in.readInt();
        for (int i = 0; i < fieldCount; i++) {
            int index = in.readInt();
            String name = in.readString();
            FieldType type = FieldType.get(in.readByte());
            int fieldFactoryId = in.readInt();
            int fieldClassId = in.readInt();
            int fieldVersion = in.readInt();

            FieldDefinitionImpl fieldDefinition =
                    new FieldDefinitionImpl(index, name, type, fieldFactoryId, fieldClassId, fieldVersion);
            classDefinition.addField(fieldDefinition);
        }

        this.classDefinition = classDefinition.build();
    }
}
