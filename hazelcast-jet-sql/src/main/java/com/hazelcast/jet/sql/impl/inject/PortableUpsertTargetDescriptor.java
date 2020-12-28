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
import com.hazelcast.internal.serialization.impl.portable.ClassDefinitionImpl;
import com.hazelcast.internal.serialization.impl.portable.FieldDefinitionImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ClassDefinition;
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
        for (String fieldName : classDefinition.getFieldNames()) {
            FieldDefinitionImpl fd = (FieldDefinitionImpl) classDefinition.getField(fieldName);
            out.writeInt(fd.getIndex());
            out.writeUTF(fd.getName());
            out.writeByte(fd.getType().getId());
            out.writeInt(fd.getFactoryId());
            out.writeInt(fd.getClassId());
            out.writeInt(fd.getVersion());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int factoryId = in.readInt();
        int classId = in.readInt();
        int version = in.readInt();
        classDefinition = new ClassDefinitionImpl(factoryId, classId, version);
        int fieldCount = in.readInt();
        for (int i = 0; i < fieldCount; i++) {
            int index = in.readInt();
            String fieldName = in.readUTF();
            byte fieldType = in.readByte();
            int fieldFactoryId = in.readInt();
            int fieldClassId = in.readInt();
            int fieldVersion = in.readInt();
            FieldType type = FieldType.get(fieldType);
            FieldDefinitionImpl fd = new FieldDefinitionImpl(index, fieldName, type, fieldFactoryId, fieldClassId, fieldVersion);
            ((ClassDefinitionImpl) classDefinition).addFieldDef(fd);
        }
    }
}
