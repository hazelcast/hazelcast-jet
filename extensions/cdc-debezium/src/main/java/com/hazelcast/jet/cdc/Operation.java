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
    SYNC('r'),
    /**
     * Record insertion, sourced from the DB changelog.
     */
    INSERT('c'),
    /**
     * Record update, sourced from the DB changelog.
     */
    UPDATE('u'),
    /**
     * Record deletion, sourced from the DB changelog.
     */
    DELETE('d');

    private static final int OFFSET;
    private static final Operation[] ARRAY;

    static {
        int min = Character.MAX_VALUE;
        int max = Character.MIN_VALUE;
        for (Operation op : values()) {
            if (op.id == null) {
                continue;
            }
            min = op.id < min ? op.id : min;
            max = op.id > max ? op.id : max;
        }

        OFFSET = min;

        ARRAY = new Operation[max - min + 1];
        for (Operation op : values()) {
            if (op.id == null) {
                continue;
            }
            ARRAY[op.id - OFFSET] = op;
        }
    }

    private final Character id;

    Operation(Character id) {
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
    public static Operation get(@Nullable String string) throws ParsingException {
        if (string == null) {
            return UNSPECIFIED;
        }

        if (string.length() == 1) {
            char id = string.charAt(0);
            Operation op = ARRAY[id - OFFSET];
            if (op != null) {
                return op;
            }
        }

        throw new ParsingException("'" + string + "' is not a valid operation id");
    }
}
