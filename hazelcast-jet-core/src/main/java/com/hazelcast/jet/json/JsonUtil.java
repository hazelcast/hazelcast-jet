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

package com.hazelcast.jet.json;

import com.fasterxml.jackson.jr.ob.JSON;
import com.fasterxml.jackson.jr.ob.JSON.Feature;
import com.hazelcast.core.HazelcastJsonValue;

import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;

public final class JsonUtil {

    private JsonUtil() {
    }

    /**
     * Creates a {@link HazelcastJsonValue} by converting given the object to
     * string using {@link Object#toString()}.
     */
    public static <T> HazelcastJsonValue hazelcastJsonValue(T object) {
        return new HazelcastJsonValue(object.toString());
    }

    /**
     * Creates a {@link HazelcastJsonValue} by converting the key of the given
     * entry to string using {@link Object#toString()}.
     */
    public static <K> HazelcastJsonValue asJsonKey(Map.Entry<K, ?> entry) {
        return new HazelcastJsonValue(entry.getKey().toString());
    }

    /**
     * Creates a {@link HazelcastJsonValue} by converting the value of the
     * given entry to string using {@link Object#toString()}.
     */
    public static <V> HazelcastJsonValue asJsonValue(Map.Entry<?, V> entry) {
        return new HazelcastJsonValue(entry.getValue().toString());
    }

    /**
     * Converts a JSON string to a object of given type.
     */
    public static <T> T parse(Class<T> type, String jsonString) {
        return uncheckCall(() -> JSON.std.beanFrom(type, jsonString));
    }

    /**
     * Converts a JSON string to a {@link Map}.
     */
    public static Map<String, Object> parse(String jsonString) {
        return uncheckCall(() -> JSON.std.with(Feature.READ_ONLY).mapFrom(jsonString));
    }

    /**
     * Extracts a string value from given JSON string.
     */
    public static String getString(String jsonString, String key) {
        return (String) parse(jsonString).get(key);
    }

    /**
     * Extracts an integer value from given JSON string.
     */
    public static int getInt(String jsonString, String key) {
        return (int) parse(jsonString).get(key);
    }

    /**
     * Extracts a boolean value from given JSON string.
     */
    public static boolean getBoolean(String jsonString, String key) {
        return (boolean) parse(jsonString).get(key);
    }

    /**
     * Extracts an array value as a {@link List} from given JSON string.
     */
    public static List<Object> getList(String jsonString, String key) {
        return (List) parse(jsonString).get(key);
    }

    /**
     * Extracts an array value as a {@link Object Object[]} from given JSON string.
     */
    public static Object[] getArray(String jsonString, String key) {
        Map<String, Object> map = uncheckCall(() -> JSON.std.with(Feature.READ_ONLY)
                                                            .with(Feature.READ_JSON_ARRAYS_AS_JAVA_ARRAYS)
                                                            .mapFrom(jsonString));
        return (Object[]) map.get(key);
    }

    /**
     * Extracts an object as a {@link Map} from given JSON string.
     */
    public static Map<String, Object> getObject(String jsonString, String key) {
        return (Map<String, Object>) parse(jsonString).get(key);
    }

    /**
     * Creates a JSON string for the given object.
     */
    public static <T> String asString(T object) {
        return uncheckCall(() -> JSON.std.asString(object));
    }

}
