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

package com.hazelcast.jet.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.hazelcast.jet.annotation.EvolvingApi;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * Arbitrary part of a {@link ChangeEvent}, as big as the whole body orz
 * as small as a single patch expression, based on a complete JSON
 * expression. Contains various methods for retrieving component values
 * or for mapping itself to data objects.
 *
 * @since 4.1
 */
@EvolvingApi
public interface ChangeEventElement extends Serializable { //todo: use better serialization

    /**
     * Maps the entire element to an instance of the specified class.
     * <p>
     * For databases providing standard JSON syntax, parsing it is based
     * on <a href="https://github.com/FasterXML/jackson-databind">Jackson Databind</a>,
     * in particular on the Jackson {@code ObjectMapper}, so the
     * parameter class needs to be annotated accordingly.
     * <p>
     * Certain databases have limitations, for example MongoDB, which
     * uses an extended JSON syntax, mapping is only supported to
     * instances of the {@code org.bson.Document} class.
     *
     * @return optional {@code Object} value, which is empty only if
     * the specified key is not found or if it's value is null
     * @throws ParsingException if the whole structure containing this
     *                          element is unparsable or the mapping
     *                          fails to produce a result
     */
    @Nonnull
    <T> T mapToObj(Class<T> clazz) throws ParsingException;

    /**
     * Best effor method for finding the specified (top level) key in
     * the underlying JSON message and returning its value as is,
     * without attempting to parse it in any way. This means that it
     * can return objects specific to the parsing used by internal
     * implementations, so Jackson classes (mostly {@link JsonNode}
     * implementations), or MongoDB Java driver classes (for example
     * embedded {@code org.bson.Document} instances).
     * <p>
     * Should not be used normally, is intended as a fallback in case
     * regular parsing fails for some reason.
     *
     * @throws ParsingException if the whole structure containing this
     *                          element or the element itself is unparsable
     */
    @Nonnull
    Optional<Object> getObject(String key) throws ParsingException;

    /**
     * Best effort method for finding the specified (top level) key in
     * the underlying JSON message and returning its value as a
     * {@link String}, but only as long as the value is indeed just a
     * simple value. So numbers, strings, booleans will be converted,
     * but complex structures like arrays and collections won't.
     *
     * @return optional {@code String} value, which is empty if the
     * specified key is not found or if the value is null or doesn't
     * represent a single value.
     * @throws ParsingException if the underlying JSON message, or any
     *                          of its parent messages are unparsable
     */
    @Nonnull
    Optional<String> getString(String key) throws ParsingException;

    /**
     * Best effort method for finding the specified (top level) key in
     * the underlying JSON message and returning its value in the form
     * of an {@link Integer}, but only as long as the value is indeed
     * a number or a string that can be parsed into one.
     *
     * @return optional {@code Integer} value, which is empty if the
     * specified key is not found or if the value is null or if it isn't
     * a number or a string that can be parsed as a number
     * @throws ParsingException if the underlying JSON message, or any
     *                          of its parent messages are unparsable
     */
    @Nonnull
    Optional<Integer> getInteger(String key) throws ParsingException;

    /**
     * Best effort method for finding the specified (top level) key in
     * the underlying JSON message and returning its value in the form
     * of a {@link Long}, but only as long as the value is indeed
     * a number or a string that can be parsed into one.
     *
     * @return optional {@code Long} value, which is empty if the
     * specified key is not found or if the value is null or if it isn't
     * a number or a string that can be parsed as a number
     * @throws ParsingException if the underlying JSON message, or any
     *                          of its parent messages are unparsable
     */
    @Nonnull
    Optional<Long> getLong(String key) throws ParsingException;

    /**
     * Best effort method for finding the specified (top level) key in
     * the underlying JSON message and returning its value in the form
     * of a {@link Double}, but only as long as the value is indeed
     * a number or a string that can be parsed into one.
     *
     * @return optional {@code Double} value, which is empty if the
     * specified key is not found or if the value is null or if it isn't
     * a number or a string that can be parsed as a number
     * @throws ParsingException if the underlying JSON message, or any
     *                          of its parent messages are unparsable
     */
    @Nonnull
    Optional<Double> getDouble(String key) throws ParsingException;

    /**
     * Best effort method for finding the specified (top level) key in
     * the underlying JSON message and returning its value in the form
     * of a {@link Boolean}, but only as long as the value is indeed
     * a boolean or a string that can be parsed into one.
     *
     * @return optional {@code Boolean} value, which is empty if the
     * specified key is not found or if the value is null or if it isn't
     * a boolean or a string that can be parsed as a boolean
     * @throws ParsingException if the underlying JSON message, or any
     *                          of its parent messages are unparsable
     */
    @Nonnull
    Optional<Boolean> getBoolean(String key) throws ParsingException;

    /**
     * Best effort method for finding the specified (top level) key in
     * the underlying JSON message and returning its value a
     * {@link List} of optional values of a certain type.
     *
     * @param <T> type of elements in the list
     * @return optional {@code List} value, which is empty only if the
     * specified key is not found or if the value is null or not a
     * collection type; the optional {@code T} elements in the list
     * are empty if they are null or can't be parsed as the specified
     * type
     * @throws ParsingException if the underlying JSON message, or any
     *                          of its parent messages are unparsable
     */
    @Nonnull
    <T> Optional<List<Optional<T>>> getList(String key, Class<T> clazz) throws ParsingException;

    /**
     * Returns raw JSON string which the content of this event element
     * is based on. To be used when parsing fails for some reason
     * (for example on some untested DB-connector version combination).
     * <p>
     * While the format is standard for RELATIONAL DATABASES, for
     * MongoDB it's MongoDB Extended JSON v2 format and needs to be
     * parsed accordingly.
     */
    @Nonnull
    String asJson();

}
