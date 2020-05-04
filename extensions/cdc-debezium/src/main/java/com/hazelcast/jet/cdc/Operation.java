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

import com.hazelcast.jet.annotation.EvolvingApi;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Describes the nature of the event in CDC data. Equivalent to various
 * actions that can affect a database record: insertion, update and
 * deletion. Has some extra special values like "sync" which is just
 * like an insert, but originates from a database snapshot (as opposed
 * database changelog) and "unspecified" which is used for a few special
 * CDC events, like heartbeats.
 *
 * @since 4.2
 */
@EvolvingApi
public enum Operation {
    /**
     * {@code ChangeRecord} doesn't have an operation field, for example
     * heartbeats.
     */
    UNSPECIFIED(null),
    /**
     * Just like {@link #INSERT}, but coming from the DB snapshot (as
     * opposed to trailing the DB changelog).
     */
    SYNC("r"),
    /**
     * Record insertion, sourced from the DB changelog.
     */
    INSERT("c"),
    /**
     * Record update, sourced from the DB changelog.
     */
    UPDATE("u"),
    /**
     * Record deletion, sourced from the DB changelog.
     */
    DELETE("d");

    private final String id;

    Operation(String id) {
        this.id = id;
    }

    /**
     * Parses the string constants used in CDC messages for describing
     * operations into enum instances.
     * <p>
     * Null will be parsed as {@link #UNSPECIFIED}.
     *
     * @throws ParsingException if the input string doesn't represent
     * an expected value.
     */
    public static Operation get(@Nullable String id) throws ParsingException {
        Operation[] values = values();
        for (Operation value : values) {
            if (Objects.equals(value.id, id)) {
                return value;
            }
        }
        throw new ParsingException("'" + id + "' is not a valid operation id");
    }
}
