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
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.jet.pipeline.BatchStage;

import java.util.Map.Entry;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.Util.entry;

/**
 * todo add proper javadoc
 */
public final class JsonUtil {

    private JsonUtil() {
    }

    /* map to HazelcastJsonValue */

    public static <T> FunctionEx<? super T, Entry<HazelcastJsonValue, HazelcastJsonValue>>
    mapHazelcastJsonValue(
            FunctionEx<? super T, String> toKeyFn,
            FunctionEx<? super T, String> toValueFn
    ) {
        return object -> entry(hazelcastJsonValue(toKeyFn.apply(object)), hazelcastJsonValue(toValueFn.apply(object)));
    }

    public static <K, V> FunctionEx<? super Entry<? super K, ? super V>,
            Entry<HazelcastJsonValue, HazelcastJsonValue>>
    mapHazelcastJsonValue() {
        return mapHazelcastJsonValue(e -> e.getKey().toString(), e -> e.getValue().toString());
    }

    /* transform to HazelcastJsonValue */

    public static <T> FunctionEx<BatchStage<? extends T>,
            BatchStage<Entry<HazelcastJsonValue, HazelcastJsonValue>>>
    transformHazelcastJsonValue(
            FunctionEx<? super T, String> toKeyFn,
            FunctionEx<? super T, String> toValueFn
    ) {
        return upstream -> upstream.map(mapHazelcastJsonValue(toKeyFn, toValueFn));
    }

//    public static <K, V> FunctionEx<BatchStage<? extends Entry<? super K, ? super V>>,
//            BatchStage<Entry<HazelcastJsonValue, HazelcastJsonValue>>>
//    transformHazelcastJsonValue() {
//        return upstream -> upstream.map(mapHazelcastJsonValue());
//    }

    public static <T> HazelcastJsonValue hazelcastJsonValue(T object) {
        return new HazelcastJsonValue(object.toString());
    }

    public static <K, V> FunctionEx<Entry<K, V>, ?> wrapKey(boolean json) {
        return entry -> json ? hazelcastJsonValue(entry.getKey()) : entry.getKey();
    }


    /* Map and FlatMap Functions*/

    public static <T> FunctionEx<? super T, JsonValue> jsonValueMapFn() {
        return object -> Json.parse(object.toString());
    }

    public static <T> FunctionEx<? super T, JsonObject> jsonObjectMapFn() {
        return object -> Json.parse(object.toString()).asObject();
    }

    public static <T> FunctionEx<? super T, Stream<JsonValue>> jsonValueFlatMapFn() {
        return object -> {
            JsonValue jsonValue = Json.parse(object.toString());
            if (jsonValue.isArray()) {
                return StreamSupport.stream(jsonValue.asArray().spliterator(), false);
            }
            return Stream.of(jsonValue);
        };
    }

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
