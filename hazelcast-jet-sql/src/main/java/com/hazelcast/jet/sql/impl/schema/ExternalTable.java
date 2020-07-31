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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * User-defined table schema definition.
 */
public class ExternalTable implements DataSerializable {

    private String name;
    private String type;
    private List<ExternalField> externalFields;
    private Map<String, String> options;

    @SuppressWarnings("unused")
    private ExternalTable() {
    }

    public ExternalTable(
            String name,
            String type,
            List<ExternalField> externalFields,
            Map<String, String> options
    ) {
        Set<String> fieldNames = new HashSet<>();
        Set<String> externalFieldNames = new HashSet<>();
        for (ExternalField field : externalFields) {
            if (!fieldNames.add(field.name())) {
                throw new IllegalArgumentException("Column '" + field.name()
                        + "' specified more than once");
            }
            if (field.externalName() != null && !externalFieldNames.add(field.externalName())) {
                throw new IllegalArgumentException("Column with external name '" + field.externalName()
                        + "' specified more than once");
            }
        }

        this.name = name;
        this.type = type;
        this.externalFields = externalFields;
        this.options = options;
    }

    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public List<ExternalField> fields() {
        return Collections.unmodifiableList(externalFields);
    }

    public Map<String, String> options() {
        return Collections.unmodifiableMap(options);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(type);
        out.writeObject(externalFields);
        out.writeObject(options);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        type = in.readUTF();
        externalFields = in.readObject();
        options = in.readObject();
    }
}
