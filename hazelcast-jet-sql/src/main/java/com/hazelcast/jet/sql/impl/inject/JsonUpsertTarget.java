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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;

@NotThreadSafe
class JsonUpsertTarget implements UpsertTarget {

    private static final JsonFactory JSON_FACTORY = new ObjectMapper().getFactory();

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private JsonGenerator jsonGen;

    JsonUpsertTarget() {
    }

    @Override
    @SuppressWarnings("checkstyle:ReturnCount")
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            return FAILING_TOP_LEVEL_INJECTOR;
        }

        return value -> {
            try {
                if (value == null) {
                    jsonGen.writeNullField(path);
                } else {
                    switch (type.getTypeFamily()) {
                        case BOOLEAN:
                            jsonGen.writeBooleanField(path, (Boolean) value);
                            break;
                        case TINYINT:
                            jsonGen.writeNumberField(path, (Byte) value);
                            break;
                        case SMALLINT:
                            jsonGen.writeNumberField(path, (Short) value);
                            break;
                        case INTEGER:
                            jsonGen.writeNumberField(path, (Integer) value);
                            break;
                        case BIGINT:
                            jsonGen.writeNumberField(path, (Long) value);
                            break;
                        case REAL:
                            jsonGen.writeNumberField(path, (Float) value);
                            break;
                        case DOUBLE:
                            jsonGen.writeNumberField(path, (Double) value);
                            break;
                        case DECIMAL:
                        case TIME:
                        case DATE:
                        case TIMESTAMP:
                        case TIMESTAMP_WITH_TIME_ZONE:
                        case VARCHAR:
                            jsonGen.writeStringField(path, (String) QueryDataType.VARCHAR.convert(value));
                            break;
                        case OBJECT:
                            injectObject(path, value);
                            break;
                        default:
                            throw QueryException.error("Unsupported type: " + type);
                    }
                }
            } catch (IOException e) {
                throw sneakyThrow(e);
            }
        };
    }

    private void injectObject(String path, Object value) throws IOException {
        jsonGen.writeFieldName(path);
        if (value == null) {
            jsonGen.writeNull();
        } else if (value instanceof TreeNode) {
            jsonGen.writeTree((TreeNode) value);
        } else if (value instanceof Boolean) {
            jsonGen.writeBoolean((boolean) value);
        } else if (value instanceof Byte) {
            jsonGen.writeNumber((byte) value);
        } else if (value instanceof Short) {
            jsonGen.writeNumber((short) value);
        } else if (value instanceof Integer) {
            jsonGen.writeNumber((int) value);
        } else if (value instanceof Long) {
            jsonGen.writeNumber((long) value);
        } else if (value instanceof Float) {
            jsonGen.writeNumber((float) value);
        } else if (value instanceof Double) {
            jsonGen.writeNumber((double) value);
        } else {
            jsonGen.writeString((String) QueryDataType.VARCHAR.convert(value));
        }
    }

    @Override
    public void init() {
        baos.reset();
        try {
            jsonGen = JSON_FACTORY.createGenerator(baos);
            jsonGen.writeStartObject();
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public Object conclude() {
        try {
            jsonGen.writeEndObject();
            jsonGen.close();
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
        return baos.toByteArray();
    }
}
