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

package com.hazelcast.jet.cdc.mongodb;

import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.impl.util.LazyThrowingSupplier;
import com.hazelcast.jet.cdc.impl.util.ThrowingSupplier;
import org.bson.Document;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public final class MongoParsing {

    private MongoParsing() {
    }

    public static ThrowingSupplier<Document, ParsingException> parse(String json) {
        return () -> {
            try {
                return Document.parse(json);
            } catch (Exception e) {
                throw new ParsingException(e.getMessage(), e);
            }
        };
    }

    public static ThrowingSupplier<Optional<Document>, ParsingException> getDocument(Document parent, String key) {
        return new LazyThrowingSupplier<>(
                () -> {
                    Optional<String> json = getString(parent, key);
                    Document result;
                    try {
                        result = Document.parse(json.get());
                    } catch (Exception e) {
                        throw new ParsingException(e.getMessage(), e);
                    }
                    return json.isPresent() ? Optional.of(result) : Optional.empty();
                }
        );
    }

    public static <T> Optional<List<Optional<T>>> getList(Document document, String key, Class<T> clazz) {
        Object value = document.get(key);
        return getList(clazz, value);
    }

    public static Optional<Object> getObject(Document document, String key) {
        Object object = document.get(key);
        return getObject(object);
    }

    public static Optional<String> getString(Document document, String key) {
        Object object = document.get(key);
        return getString(object);
    }

    public static Optional<Integer> getInteger(Document document, String key) {
        Object object = document.get(key);
        return getInteger(object);
    }

    public static Optional<Long> getLong(Document document, String key) {
        Object object = document.get(key);
        return getLong(object);
    }

    public static Optional<Double> getDouble(Document document, String key) {
        Object object = document.get(key);
        return getDouble(object);
    }

    public static Optional<Boolean> getBoolean(Document document, String key) {
        Object object = document.get(key);
        return getBoolean(object);
    }

    private static Optional<Object> getObject(Object object) {
        return Optional.ofNullable(object);
    }

    private static Optional<String> getString(Object object) {
        if (object instanceof String) {
            return Optional.of((String) object);
        } else if (object instanceof Number) {
            return Optional.of(object.toString());
        } else if (object instanceof Boolean) {
            return Optional.of(object.toString());
        } else if (object instanceof Date) {
            return Optional.of(object.toString());
        } else {
            return Optional.empty();
        }
    }

    private static Optional<Integer> getInteger(Object object) {
        if (object instanceof Number) {
            return Optional.of(((Number) object).intValue());
        } else if (object instanceof String) {
            String stringValue = (String) object;
            try {
                return Optional.of(Integer.valueOf(stringValue));
            } catch (NumberFormatException ei) {
                try {
                    return Optional.of(Double.valueOf(stringValue).intValue());
                } catch (NumberFormatException ed) {
                    return Optional.empty();
                }
            }
        } else {
            return Optional.empty();
        }
    }

    private static Optional<Long> getLong(Object object) {
        if (object instanceof Number) {
            return Optional.of(((Number) object).longValue());
        } else if (object instanceof String) {
            String stringValue = (String) object;
            try {
                return Optional.of(Long.valueOf(stringValue));
            } catch (NumberFormatException ei) {
                try {
                    return Optional.of(Double.valueOf(stringValue).longValue());
                } catch (NumberFormatException ed) {
                    return Optional.empty();
                }
            }
        } else {
            return Optional.empty();
        }
    }

    private static Optional<Double> getDouble(Object object) {
        if (object instanceof Number) {
            return Optional.of(((Number) object).doubleValue());
        } else if (object instanceof String) {
            try {
                return Optional.of(Double.valueOf((String) object));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    private static Optional<Boolean> getBoolean(Object object) {
        if (object instanceof Boolean) {
            return Optional.of(((Boolean) object));
        } else if (object instanceof String) {
            return Optional.of(Boolean.valueOf((String) object));
        } else {
            return Optional.empty();
        }
    }

    private static <T> Optional<List<Optional<T>>> getList(Class<T> clazz, Object value) {
        if (value instanceof List) {
            return Optional.of(((List<?>) value).stream()
                    .map(e -> {
                        if (clazz.equals(String.class)) {
                            return (Optional<T>) getString(e);
                        } else if (clazz.equals(Integer.class)) {
                            return (Optional<T>) getInteger(e);
                        } else if (clazz.equals(Long.class)) {
                            return (Optional<T>) getLong(e);
                        } else if (clazz.equals(Double.class)) {
                            return (Optional<T>) getDouble(e);
                        } else if (clazz.equals(Boolean.class)) {
                            return (Optional<T>) getBoolean(e);
                        } else if (clazz.equals(Object.class)) {
                            return (Optional<T>) getObject(e);
                        } else {
                            throw new IllegalArgumentException(clazz.getName() + " not supported");
                        }
                    })
                    .collect(Collectors.toList()));
        }
        return Optional.empty();
    }

}
