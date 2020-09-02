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

import com.hazelcast.jet.sql.impl.parse.SqlDataType;
import com.hazelcast.jet.sql.impl.parse.SqlMappingColumn;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MappingField implements DataSerializable {

    private static final String NAME = "name";
    private static final String TYPE = "type";
    private static final String EXTERNAL_NAME = "externalName";

    // This generic structure is used to have binary compatibility if more fields are added in
    // the future, like nullability, watermark info etc. Instances are stored as a part of the
    // persisted schema.
    private Map<String, Object> properties;

    @SuppressWarnings("unused")
    private MappingField() {
    }

    public MappingField(String name, QueryDataType type) {
        this(name, type, null);
    }

    public MappingField(String name, QueryDataType type, String externalName) {
        this.properties = new HashMap<>();
        this.properties.put(NAME, Objects.requireNonNull(name));
        this.properties.put(TYPE, Objects.requireNonNull(type));
        if (externalName != null) {
            this.properties.put(EXTERNAL_NAME, externalName);
        }
    }

    /**
     * Column name. This is the name that is seen in SQL queries.
     */
    public String name() {
        return Objects.requireNonNull((String) properties.get(NAME), "missing name property");
    }

    public QueryDataType type() {
        return Objects.requireNonNull((QueryDataType) properties.get(TYPE), "missing type property");
    }

    /**
     * The external name of a field. For example, in case of IMap or Kafka,
     * it always starts with `__key` or `this`.
     */
    public String externalName() {
        return (String) properties.get(EXTERNAL_NAME);
    }

    public SqlMappingColumn toSqlColumn() {
        SqlParserPos z = SqlParserPos.ZERO;
        QueryDataType type = type();
        String externalName = externalName();
        return new SqlMappingColumn(
                new SqlIdentifier(name(), z),
                type == null ? null : new SqlDataType(type(), z),
                externalName == null ? null : new SqlIdentifier(externalName(), z),
                z
        );
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(properties);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        properties = in.readObject();
    }
}
