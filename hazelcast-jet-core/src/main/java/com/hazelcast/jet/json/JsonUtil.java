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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
     * Returns a function which converts the given input to string using
     * {@link Object#toString()} and parses it to {@link JsonValue}. The
     * function can be used in mapping stages like
     * {@link BatchStage#map(FunctionEx)}, {@link StreamStage#map(FunctionEx)}.
     */
    public static <T> FunctionEx<? super T, JsonValue> jsonValueMapFn() {
        return object -> Json.parse(object.toString());
    }

    /**
     * Convenience for {@link #jsonValueMapFn()}, which further converts the
     * {@link JsonValue} to {@link JsonObject}.
     */
    public static <T> FunctionEx<? super T, JsonObject> jsonObjectMapFn() {
        return object -> Json.parse(object.toString()).asObject();
    }

    /**
     * Returns a function which converts the given input to string using
     * {@link Object#toString()} and parses it to {@link JsonValue}. If the
     * parsed object is a {@link JsonArray}, returns the objects in the array
     * as a stream of {@link JsonValue}s. Otherwise returns the parsed object
     * as a stream. The function can be used in flat-mapping stages like
     * {@link BatchStage#flatMap(FunctionEx)},
     * {@link StreamStage#flatMap(FunctionEx)}}.
     */
    public static <T> FunctionEx<? super T, Stream<JsonValue>> jsonValueFlatMapFn() {
        return object -> {
            JsonValue jsonValue = Json.parse(object.toString());
            if (jsonValue.isArray()) {
                return StreamSupport.stream(jsonValue.asArray().spliterator(), false);
            }
            return Stream.of(jsonValue);
        };
    }

    /**
     * Convenience for {@link #jsonValueFlatMapFn()}, which further converts the
     * {@link JsonValue} to {@link JsonObject}.
     */
    public static <T> FunctionEx<? super T, Stream<JsonObject>> jsonObjectFlatMapFn() {
        return object -> {
            JsonValue jsonValue = Json.parse(object.toString());
            if (jsonValue.isArray()) {
                return StreamSupport.stream(jsonValue.asArray().spliterator(), false)
                                    .map(JsonValue::asObject);
            }
            return Stream.of(jsonValue.asObject());
        };
    }

}
