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

package com.hazelcast.jet.sql.impl.extract;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.type.QueryDataType;

public class JsonQueryTarget implements QueryTarget {

    private JsonObject json;

    @Override
    public void setTarget(Object target) {
        json = Json.parse((String) target).asObject();
    }

    @Override
    public QueryExtractor createExtractor(String path, QueryDataType type) {
        return () -> type.convert(extractValue(json, path));
    }

    private static Object extractValue(JsonObject json, String path) {
        JsonValue value = json.get(path);
        if (value == null || value.isNull()) {
            return null;
        } else if (value.isBoolean()) {
            return value.asBoolean();
        } else if (value.isNumber()) {
            if (value.toString().contains(".")) {
                return value.asDouble();
            } else {
                return value.asLong();
            }
        } else if (value.isString()) {
            return value.asString();
        } else {
            return value;
        }
    }
}
