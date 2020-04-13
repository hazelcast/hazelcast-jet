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

package com.hazelcast.jet.impl.json;

import com.hazelcast.com.fasterxml.jackson.core.JsonFactory;
import com.hazelcast.com.fasterxml.jackson.core.JsonParser;
import com.hazelcast.com.fasterxml.jackson.core.JsonToken;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.jet.impl.util.ExceptionUtil;

import java.io.IOException;
import java.io.Reader;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.hazelcast.com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.hazelcast.com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * A JSON parser which parses the given {@link Reader input} as a
 * stream of {@link JsonObject}s. The actual parsing happens while the
 * returned {@linkplain Stream} is being consumed.
 * <p>
 * If the input contains a single object, a stream containing that
 * single object is returned. Otherwise, parser assumes the input is an
 * {@link JsonArray array of objects} and returns a stream which
 * iterates over these objects.
 * <p>
 * The parser does not validate the content. If the provided content
 * contains an invalid JSON the result is unpredictable, it may or may
 * not throw an exception.When the stream closed, provided
 * {@code reader} is also closed.
 */
public class StreamJsonParser implements Spliterator<JsonObject> {

    private static final JsonFactory FACTORY = new JsonFactory();

    private final JsonParser parser;

    private final boolean singleObject;

    public StreamJsonParser(Reader reader) throws IOException {
        this.parser = FACTORY.createParser(reader);
        singleObject = START_OBJECT.equals(parser.nextToken());
    }

    public Stream<JsonObject> stream() {
        if (singleObject) {
            return Stream.of(uncheckCall(this::parseObject)).onClose(() -> uncheckRun(parser::close));
        }
        return StreamSupport.stream(this, false).onClose(() -> uncheckRun(parser::close));
    }

    @Override
    public boolean tryAdvance(Consumer<? super JsonObject> action) {
        try {
            JsonToken jsonToken = parser.nextToken();
            if (END_ARRAY.equals(jsonToken)) {
                parser.close();
                return false;
            }
            action.accept(parseObject());
            return true;
        } catch (IOException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private JsonObject parseObject() throws IOException {
        JsonObject object = new JsonObject();
        JsonToken jsonToken = parser.nextToken();
        String currentName = parser.getCurrentName();
        while (!END_OBJECT.equals(jsonToken)) {
            switch (jsonToken) {
                case START_ARRAY:
                    object.add(currentName, parseArray());
                    break;
                case FIELD_NAME:
                    currentName = parser.getCurrentName();
                    break;
                case START_OBJECT:
                    object.add(currentName, parseObject());
                    break;
                case VALUE_STRING:
                    object.add(currentName, parser.getValueAsString());
                    break;
                case VALUE_NUMBER_INT:
                    object.add(currentName, parser.getValueAsInt());
                    break;
                case VALUE_NUMBER_FLOAT:
                    object.add(currentName, parser.getValueAsDouble());
                    break;
                case VALUE_FALSE:
                    object.add(currentName, false);
                    break;
                case VALUE_TRUE:
                    object.add(currentName, true);
                    break;
                case VALUE_NULL:
                    object.add(currentName, Json.value(null));
                    break;
                default:
                    throw new IllegalArgumentException("Unknown token: " + jsonToken);
            }
            jsonToken = parser.nextToken();
        }
        return object;
    }

    private JsonArray parseArray() throws IOException {
        JsonArray jsonArray = new JsonArray();
        JsonToken jsonToken = parser.nextToken();
        while (!END_ARRAY.equals(jsonToken)) {
            switch (jsonToken) {
                case START_OBJECT:
                    jsonArray.add(parseObject());
                    break;
                case VALUE_STRING:
                    jsonArray.add(parser.getValueAsString());
                    break;
                case VALUE_NUMBER_INT:
                    jsonArray.add(parser.getValueAsInt());
                    break;
                case VALUE_NUMBER_FLOAT:
                    jsonArray.add(parser.getValueAsDouble());
                    break;
                case VALUE_FALSE:
                    jsonArray.add(false);
                    break;
                case VALUE_TRUE:
                    jsonArray.add(true);
                    break;
                case VALUE_NULL:
                    jsonArray.add(Json.value(null));
                    break;
                default:
                    throw new IllegalArgumentException("Unknown token: " + jsonToken);
            }
            jsonToken = parser.nextToken();
        }
        return jsonArray;
    }

    @Override
    public Spliterator<JsonObject> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return Spliterator.ORDERED | Spliterator.NONNULL;
    }
}
