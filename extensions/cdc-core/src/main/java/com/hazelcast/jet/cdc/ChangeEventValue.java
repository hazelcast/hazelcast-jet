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
 * Value (body) portion of a {@link ChangeEvent}, describes the actual
 * change that has been done to the affected database record or
 * document.
 *
 * @since 4.1
 */
@EvolvingApi
public interface ChangeEventValue extends ChangeEventElement {

    /**
     * Specifies the moment in time when the event happened. This
     * timestamp is "real" in the sense that it comes from the database
     * change-log, so it's not "processing time", it's not the moment
     * when the event was observed by our system. Keep in mind though
     * that not all events come from the change-log. The change-log goes
     * back in time only to a limited extent, all older events are parts
     * of a database snapshot constructed when we start monitoring the
     * database and their timestamps are accordingly artificial.
     * Identifying snapshot events is possible most of the time, because
     * their operation will be {@link Operation#SYNC} instead of
     * {@link Operation#INSERT} (one notable exception being MySQL).
     *
     * @throws ParsingException if no parsable timestamp field present
     */
    long timestamp() throws ParsingException;

    /**
     * Specifies the type of change being described (insertion, delete or
     * update). Only some special events, like heartbeats don't have an
     * operation value.
     *
     * @return {@link Operation#UNSPECIFIED} if this {@code ChangeEventValue}
     * doesn't have an operation field or appropriate {@link Operation}
     * that matches what's found in the operation field
     * @throws ParsingException if there is an operation field, but it's
     *                          value is not among the handled ones.
     */
    @Nonnull
    Operation operation() throws ParsingException;

    /**
     * Describes how the database record looked like BEFORE applying the
     * change event.
     *
     * @throws ParsingException if this {@code ChangeEventValue} doesn't
     *                          have a 'before' sub-element
     */
    @Nonnull
    ChangeEventElement before() throws ParsingException;

    /**
     * Describes how the database record looks like AFTER the change
     * event has been applied.
     *
     * @throws ParsingException if this {@code ChangeEventValue} doesn't
     *                          have an 'after' sub-element
     */
    @Nonnull
    ChangeEventElement after() throws ParsingException;

}
