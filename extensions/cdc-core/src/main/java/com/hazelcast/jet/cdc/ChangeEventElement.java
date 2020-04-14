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
import java.util.List;
import java.util.Optional;

/**
 * Arbitrary part of a {@link ChangeEvent}, as big as the whole body or
 * as small as a single patch expression, based on a complete JSON
 * expression. Contains various methods for retrieving component values
 * or for mapping itself to data objects.
 *
 * @since 4.1
 */
@EvolvingApi
public interface ChangeEventElement {

    /**
     * Maps the entire element to an instance of the specified class.
     * <p>
     * For databases providing standard JSON syntax, parsing it is based
     * on <a
     * href="https://github.com/FasterXML/jackson-databind">Jackson
     * Databind</a>, in particular on the Jackson {@code ObjectMapper},
     * so the parameter class needs to be annotated accordingly.
     *
     * @return optional {@code Object} value, which is empty only if the
     * specified key is not found or if it's value is null
     * @throws ParsingException if the whole structure containing this
     *                          element is unparsable or the mapping
     *                          fails to produce a result
     */
    @Nonnull
    <T> T mapToObj(Class<T> clazz) throws ParsingException;

    /**
     * Returns the value of the specified (top level) key in the
     * underlying JSON message as a child {@code ChangeEventElement}.
     *
     * @return optional {@code ChangeEventElement}, which is empty if
     * the specified key is not found or if the value is null.
     * @throws ParsingException if the underlying JSON message, or any
     *                          of its parent messages are unparsable
     */
    Optional<ChangeEventElement> getChild(String key) throws ParsingException;

    /**
     * Returns the value of the specified (top level) key in the
     * underlying JSON message as a {@link String}, but only as long as
     * the value is indeed a simple text value. So numbers, booleans,
     * complex structures like arrays and collections won't be handled
     * (returned {@code Optional} will be empty).
     *
     * @return optional {@code String} value, which is empty if the
     * specified key is not found or if the value is null or doesn't
     * represent a simple text value.
     * @throws ParsingException if the underlying JSON message, or any
     *                          of its parent messages are unparsable
     */
    @Nonnull
    Optional<String> getString(String key) throws ParsingException;

    /**
     * Returns the value of the specified (top level) key in the
     * underlying JSON message as an {@link Integer}, but only as long
     * as the value is indeed a number. Various number types (like
     * floating point, integer and so on) will all be handled, but
     * strings won't be attempted to be parsed as numbers (returned
     * {@code Optional} will be empty).
     *
     * @return optional {@code Integer} value, which is empty if the
     * specified key is not found or if the value is null or if it isn't
     * a number
     * @throws ParsingException if the underlying JSON message, or any
     *                          of its parent messages are unparsable
     */
    @Nonnull
    Optional<Integer> getInteger(String key) throws ParsingException;

    /**
     * Returns the value of the specified (top level) key in the
     * underlying JSON message as an {@link Long}, but only as long as
     * the value is indeed a number. Various number types (like floating
     * point, integer and so on) will all be handled, but strings won't
     * be attempted to be parsed as numbers (returned {@code Optional}
     * will be empty).
     *
     * @return optional {@code Long} value, which is empty if the
     * specified key is not found or if the value is null or if it isn't
     * a number
     * @throws ParsingException if the underlying JSON message, or any
     *                          of its parent messages are unparsable
     */
    @Nonnull
    Optional<Long> getLong(String key) throws ParsingException;

    /**
     * Returns the value of the specified (top level) key in the
     * underlying JSON message as an {@link Double}, but only as long as
     * the value is indeed a number. Various number types (like floating
     * point, integer and so on) will all be handled, but strings won't
     * be attempted to be parsed as numbers (returned {@code Optional}
     * will be empty).
     *
     * @return optional {@code Double} value, which is empty if the
     * specified key is not found or if the value is null or if it isn't
     * a number
     * @throws ParsingException if the underlying JSON message, or any
     *                          of its parent messages are unparsable
     */
    @Nonnull
    Optional<Double> getDouble(String key) throws ParsingException;

    /**
     * Returns the value of the specified (top level) key in the
     * underlying JSON message as an {@link Boolean}, but only as long
     * as the value is indeed of a boolean type (so not a string, not a
     * number or any other complex structures).
     *
     * @return optional {@code Boolean} value, which is empty if the
     * specified key is not found or if the value is null or if it isn't
     * a boolean
     * @throws ParsingException if the underlying JSON message, or any
     *                          of its parent messages are unparsable
     */
    @Nonnull
    Optional<Boolean> getBoolean(String key) throws ParsingException;

    /**
     * Returns the value of the specified (top level) key in the
     * underlying JSON message as a {@link List} of optional values of a
     * certain type.
     *
     * @param <T> type of elements in the list
     * @return optional {@code List} value, which is empty only if the
     * specified key is not found or if the value is null or not a
     * collection type; the optional {@code T} elements in the list are
     * empty if they are null or aren't of the specified type
     * @throws ParsingException if the underlying JSON message, or any
     *                          of its parent messages are unparsable
     */
    @Nonnull
    <T> Optional<List<Optional<T>>> getList(String key, Class<T> clazz) throws ParsingException;

    /**
     * Returns the value of the specified (top level) key in the
     * underlying JSON message AS IS, without attempting to parse it in
     * any way. This means that it can return objects specific to the
     * parsing used by internal implementations, so Jackson classes
     * (mostly {@link JsonNode} implementations).
     * <p>
     * Should not be used normally, is intended as a fallback in case
     * regular parsing fails for some reason.
     *
     * @throws ParsingException if the whole structure containing this
     *                          element or the element itself is
     *                          unparsable
     */
    @Nonnull
    Optional<Object> getRaw(String key) throws ParsingException;

    /**
     * Returns raw JSON string which the content of this event element
     * is based on. To be used when parsing fails for some reason (for
     * example on some untested DB-connector version combination).
     */
    @Nonnull
    String asJson();

}
