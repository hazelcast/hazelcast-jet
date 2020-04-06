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

import java.util.Objects;

/**
 * TODO: javadoc
 *
 * @since 4.1
 */
@EvolvingApi
public enum Operation {
    /**
     * Change event doesn't have an operation field, for example heartbeats.
     */
    UNSPECIFIED(null),
    /**
     * Just like {@link #INSERT}, but coming from the DB snapshot (as
     * opposed to trailing the DB changelog).
     * TODO: javadoc
     */
    SYNC("r"),
    /**
     * Record insertion, sourced from the DB changelog.
     * TODO: javadoc
     */
    INSERT("c"),
    /**
     * Record update, sourced from the DB changelog.
     * TODO: javadoc
     */
    UPDATE("u"),
    /**
     * Record deletion, sourced from the DB changelog.
     * TODO: javadoc
     */
    DELETE("d");

    private final String id;

    Operation(String id) {
        this.id = id;
    }

    /**
     * TODO: javadoc
     */
    public static Operation get(String id) throws ParsingException {
        Operation[] values = values();
        for (Operation value : values) {
            if (Objects.equals(value.id, id)) {
                return value;
            }
        }
        throw new ParsingException("'" + id + "' is not a valid operation id");
    }
}
