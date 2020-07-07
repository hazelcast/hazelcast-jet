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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;

// TODO: review/improve/even remove in favor of IMDG JsonQueryTarget ?
public class JsonQueryTarget implements QueryTarget {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private JsonNode json;

    @Override
    public void setTarget(Object target) {
        try {
            json = OBJECT_MAPPER.readTree((String) target);
        } catch (IOException e) {
            throw QueryException.error("Unable to parse json: " + e.getMessage(), e);
        }
    }

    @Override
    public QueryExtractor createExtractor(String path, QueryDataType type) {
        return () -> type.convert(extractValue(json, path));
    }

    private static Object extractValue(JsonNode json, String path) {
        JsonNode node = json.get(path);
        if (node == null || node.isNull()) {
            return null;
        } else if (node.isBoolean()) {
            return node.asBoolean();
        } else if (node.isNumber()) {
            return node.numberValue();
        } else {
            return node.textValue();
        }
    }
}
