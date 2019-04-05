/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * All stream items in Jet jobs created using Pipeline are of this type.
 * Combines the event with timestamp and key.
 *
 * @param <T> type of the wrapped event
 * @param <K> type of the wrapped key
 */
public final class JetEvent<T, K> {
    public static final long NO_TIMESTAMP = Long.MIN_VALUE;

    @Nonnull private final T payload;
    @Nonnull private final K key;
    private final long timestamp;

    private JetEvent(@Nonnull T payload, @Nonnull K key, long timestamp) {
        this.payload = payload;
        this.key = key;
        this.timestamp = timestamp;
    }

    /**
     * Creates a new {@code JetEvent} with the given components.
     */
    @Nullable
    public static <T, K> JetEvent<T, K> jetEvent(@Nullable T payload, @Nullable K key, long timestamp) {
        if (payload == null) {
            return null;
        }
        if (key == null) {
            throw new IllegalArgumentException("Key is required");
        }
        return new JetEvent<>(payload, key, timestamp);
    }

    /**
     * Returns the timestamp of this event.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Returns the wrapped event.
     */
    @Nonnull
    public T payload() {
        return payload;
    }

    @Nonnull
    public K key() {
        return key;
    }

    @Override
    public String toString() {
        return "JetEvent{payload=" + payload + ", key=" + key + ", timestamp=" + timestamp + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JetEvent<?, ?> jetEvent = (JetEvent<?, ?>) o;
        return timestamp == jetEvent.timestamp &&
                payload.equals(jetEvent.payload) &&
                key.equals(jetEvent.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload, key, timestamp);
    }
}
