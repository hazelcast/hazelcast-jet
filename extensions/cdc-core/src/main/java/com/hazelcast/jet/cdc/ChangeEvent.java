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

import javax.annotation.Nonnull;

/**
 * Information pertaining to a single data change event (insertion,
 * delete or update), affecting either a single database record
 * (in the case of relational databases) or document (in the case of
 * NoSQL databases).
 * <p>
 * Each event has a <i>key</i>, which identifies the particular
 * record or document being affected, and a <i>value</i>, which
 * describes the actual change itself.
 * <p>
 * Most events have an <i>operation</i> associated with them which
 * specifies the type of change being described (insertion, delete or
 * update). This is really a property of the value, it's only replicated
 * at this level for convenience. Only some special events, like
 * heartbeats don't have an operation value.
 * <p>
 * There is also a <i>timestamp</i> which specifies the moment in time when
 * the event happened. This timestamp is "real" in the sense that it
 * comes from the database change-log, so it's not "processing time",
 * it's not the moment when the event was observed by our system. Keep
 * in mind though that not all events come from the change-log. The
 * change-log goes back in time only to a limited extent, all older
 * events are parts of a database snapshot constructed when we start
 * monitoring the database and their timestamps are accordingly
 * artificial. Identifying snapshot events is possible most of the time,
 * because their operation will be {@link Operation#SYNC} instead of
 * {@link Operation#INSERT} (one notable exception being MySQL).
 *
 * @since 4.1
 */
@EvolvingApi
public interface ChangeEvent {

    /**
     * Convenience method, see {@link ChangeEventValue#timestamp()} for
     * details.
     */
    default long timestamp() throws ParsingException {
        return value().timestamp();
    }

    /**
     * Convenience method, see {@link ChangeEventValue#operation()} for
     * details.
     */
    @Nonnull
    default Operation operation() throws ParsingException {
        return value().operation();
    }

    /**
     * Identifies the particular record or document being affected
     * by the change event.
     */
    @Nonnull
    ChangeEventKey key();

    /**
     * Describes the actual change affected on the record or document
     * by the change event.
     */
    @Nonnull
    ChangeEventValue value();

    /**
     * Returns raw JSON string which the content of this event is
     * based on. Mean to be used when higher level parsing (see other
     * methods) fails for some reason (for example on some untested
     * DB-connector version combination).
     */
    @Nonnull
    String asJson();
}
