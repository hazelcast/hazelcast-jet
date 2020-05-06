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

import com.fasterxml.jackson.jr.annotationsupport.JacksonAnnotationExtension;
import com.fasterxml.jackson.jr.ob.JSON;
import com.hazelcast.core.HazelcastJsonValue;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.Map;

/**
 * TODO: javadoc
 *
 * @since 4.2
 */
public final class JsonUtil {

    private static final JSON JSON_JR;

    static {
        JSON.Builder builder = JSON.builder();
        try {
            Class.forName("com.fasterxml.jackson.annotation.JacksonAnnotation", false, JsonUtil.class.getClassLoader());
            builder.register(JacksonAnnotationExtension.std);
        } catch (ClassNotFoundException ignored) {
        }
        JSON_JR = builder.build();
    }

    private JsonUtil() {
    }

    /**
     * Creates a {@link HazelcastJsonValue} by converting given the object to
     * string using {@link Object#toString()}.
     */
    @Nonnull
    public static HazelcastJsonValue hazelcastJsonValue(@Nonnull Object object) {
        return new HazelcastJsonValue(object.toString());
    }

    /**
     * Converts a JSON string to a object of given type.
     */
    @Nonnull
    public static <T> T parse(@Nonnull Class<T> type, @Nonnull String jsonString) throws IOException {
        return JSON_JR.beanFrom(type, jsonString);
    }

    /**
     * Converts the contents of the specified {@code reader} to a object of
     * given type.
     *
     * TODO: missing coverage
     */
    @Nonnull
    public static <T> T parse(@Nonnull Class<T> type, @Nonnull Reader reader) throws IOException {
        return JSON_JR.beanFrom(type, reader);
    }

    /**
     * Converts a JSON string to a {@link Map}.
     */
    @Nonnull
    public static Map<String, Object> parse(@Nonnull String jsonString) throws IOException {
        return JSON_JR.mapFrom(jsonString);
    }

    /**
     * Converts the contents of the specified {@code reader} to a {@link Map}.
     *
     * TODO: missing coverage
     */
    @Nonnull
    public static Map<String, Object> parse(@Nonnull Reader reader) throws IOException {
        return JSON_JR.mapFrom(reader);
    }

    //TODO: many missing methods, how do you read lists, or single values as json?

    /**
     * Returns an {@link Iterator} over the sequence of JSON objects parsed
     * from given {@code reader}.
     */
    @Nonnull
    public static <T> Iterator<T> parseSequence(@Nonnull Class<T> type, @Nonnull Reader reader)
            throws IOException {
        return JSON_JR.beanSequenceFrom(type, reader);
    }

    /**
     * Creates a JSON string for the given object.
     */
    @Nonnull
    public static String asJson(@Nonnull Object object) throws IOException {
        return JSON_JR.asString(object);
    }


}
