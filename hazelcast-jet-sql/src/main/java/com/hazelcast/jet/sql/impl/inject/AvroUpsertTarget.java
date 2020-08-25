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

import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecordBuilder;

// TODO: can it be non-thread safe ?
class AvroUpsertTarget implements UpsertTarget {

    private final Schema schema;

    private GenericRecordBuilder record;

    AvroUpsertTarget(String schema) {
        this.schema = new Schema.Parser().parse(schema);
    }

    @Override
    public UpsertInjector createInjector(String path) {
        if (schema.getField(path).schema().getType().equals(Type.BOOLEAN)) {
            return value -> record.set(path, value);
        } else if (schema.getField(path).schema().getType().equals(Type.INT)) {
            return value -> record.set(path, value);
        } else if (schema.getField(path).schema().getType().equals(Type.LONG)) {
            return value -> record.set(path, value);
        } else if (schema.getField(path).schema().getType().equals(Type.FLOAT)) {
            return value -> record.set(path, value);
        } else if (schema.getField(path).schema().getType().equals(Type.DOUBLE)) {
            return value -> record.set(path, value);
        } else {
            return value -> record.set(path, QueryDataType.VARCHAR.convert(value));
        }
    }

    @Override
    public void init() {
        record = new GenericRecordBuilder(schema);
    }

    @Override
    public Object conclude() {
        return record.build();
    }
}
