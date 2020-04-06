/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cdc.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.impl.util.ThrowingSupplier;

import javax.ws.rs.ProcessingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class JsonParsing {

    private JsonParsing() {
    }

    public static ThrowingSupplier<JsonNode, ParsingException> parse(String json, ObjectMapper mapper) {
        return () -> {
            try {
                return mapper.readTree(json);
            } catch (Exception e) {
                throw new ProcessingException(e.getMessage(), e);
            }
        };
    }

    public static ThrowingSupplier<Optional<JsonNode>, ParsingException> getChild(JsonNode node, String key) {
        return () -> {
            JsonNode subNode = node.get(key);
            if (subNode == null || subNode.isNull()) {
                return Optional.empty();
            } else {
                return Optional.of(subNode);
            }
        };
    }

    public static <T> T mapToObj(JsonNode node, Class<T> clazz, ObjectMapper mapper) throws ParsingException {
        try {
            return mapper.treeToValue(node, clazz);
        } catch (Exception e) {
            throw new ParsingException(e.getMessage(), e);
        }
    }

    public static <T> Optional<List<Optional<T>>> getList(JsonNode node, String key, Class<T> clazz) {
        JsonNode value = node.get(key);
        return getList(clazz, value);
    }

    public static Optional<Object> getObject(JsonNode node, String key) {
        Object value = node.get(key);
        return getObject(value);
    }

    public static Optional<String> getString(JsonNode node, String key) {
        JsonNode value = node.get(key);
        return getString(value);
    }

    public static Optional<Integer> getInteger(JsonNode node, String key) {
        JsonNode value = node.get(key);
        return getInteger(value);
    }

    public static Optional<Long> getLong(JsonNode node, String key) {
        JsonNode value = node.get(key);
        return getLong(value);
    }

    public static Optional<Double> getDouble(JsonNode node, String key) {
        JsonNode value = node.get(key);
        return getDouble(value);
    }

    public static Optional<Boolean> getBoolean(JsonNode node, String key) {
        JsonNode value = node.get(key);
        return getBoolean(value);
    }

    private static Optional<Object> getObject(Object value) {
        return Optional.ofNullable(value);
    }

    private static Optional<String> getString(JsonNode value) {
        if (value != null && value.isValueNode()) {
            return Optional.of(value.asText());
        } else {
            return Optional.empty();
        }
    }

    private static Optional<Integer> getInteger(JsonNode value) {
        if (value != null) {
            if (value.isNumber()) {
                return Optional.of(value.asInt());
            } else if (value.isValueNode()) {
                String stringValue = value.asText();
                try {
                    return Optional.of(Integer.valueOf(stringValue));
                } catch (NumberFormatException ei) {
                    try {
                        return Optional.of(Double.valueOf(stringValue).intValue());
                    } catch (NumberFormatException ef) {
                        return Optional.empty();
                    }
                }
            }
        }
        return Optional.empty();
    }

    private static Optional<Long> getLong(JsonNode value) {
        if (value != null) {
            if (value.isNumber()) {
                return Optional.of(value.asLong());
            } else if (value.isValueNode()) {
                String stringValue = value.asText();
                try {
                    return Optional.of(Long.valueOf(stringValue));
                } catch (NumberFormatException ei) {
                    try {
                        return Optional.of(Double.valueOf(stringValue).longValue());
                    } catch (NumberFormatException ef) {
                        return Optional.empty();
                    }
                }
            }
        }
        return Optional.empty();
    }

    private static Optional<Double> getDouble(JsonNode value) {
        if (value != null) {
            if (value.isNumber()) {
                return Optional.of(value.asDouble());
            } else if (value.isValueNode()) {
                String stringValue = value.asText();
                try {
                    return Optional.of(Double.valueOf(stringValue));
                } catch (NumberFormatException e) {
                    return Optional.empty();
                }
            }
        }
        return Optional.empty();
    }

    private static Optional<Boolean> getBoolean(JsonNode value) {
        if (value != null) {
            if (value.isBoolean()) {
                return Optional.of(value.asBoolean());
            } else if (value.isValueNode()) {
                String stringValue = value.asText();
                return Optional.of(Boolean.valueOf(stringValue));
            }
        }
        return Optional.empty();
    }

    private static <T> Optional<List<Optional<T>>> getList(Class<T> clazz, JsonNode value) {
        if (value != null && value.isArray()) {
            ArrayNode arrayNode = (ArrayNode) value;
            List<Optional<T>> list = new ArrayList<>(arrayNode.size());
            for (int i = 0; i < arrayNode.size(); i++) {
                Optional<T> element;
                if (clazz.equals(String.class)) {
                    element = (Optional<T>) getString(arrayNode.get(i));
                } else if (clazz.equals(Integer.class)) {
                    element = (Optional<T>) getInteger(arrayNode.get(i));
                } else if (clazz.equals(Long.class)) {
                    element = (Optional<T>) getLong(arrayNode.get(i));
                } else if (clazz.equals(Double.class)) {
                    element = (Optional<T>) getDouble(arrayNode.get(i));
                } else if (clazz.equals(Boolean.class)) {
                    element = (Optional<T>) getBoolean(arrayNode.get(i));
                } else if (clazz.equals(Object.class)) {
                    element = (Optional<T>) getObject(arrayNode.get(i));
                } else {
                    throw new IllegalArgumentException(clazz.getName() + " not supported");
                }
                list.add(element);
            }
            return Optional.of(list);
        }
        return Optional.empty();
    }

}