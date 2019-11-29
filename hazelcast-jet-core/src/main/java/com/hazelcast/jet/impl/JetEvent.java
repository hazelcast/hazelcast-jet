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
 */
public final class JetEvent<T> {
    public static final long NO_TIMESTAMP = Long.MIN_VALUE;
    public static final int UNINITIALIZED_PARTITION_ID = -1;

    private final T payload;
    private int partitionId;
    private final long timestamp;
    private transient Object key;

    private JetEvent(@Nonnull T payload, @Nullable Object key, int partitionId, long timestamp) {
        this.payload = payload;
        this.key = key;
        this.partitionId = partitionId;
        this.timestamp = timestamp;
    }

    /**
     * Creates a new {@code JetEvent} with the given components.
     */
    @Nullable
    public static <T> JetEvent<T> jetEvent(@Nullable T payload, int partitionId, long timestamp) {
        if (payload == null) {
            return null;
        }
        return new JetEvent<>(payload, null, partitionId, timestamp);
    }

    /**
     * Creates a new {@code JetEvent} with the given components.
     */
    @Nullable
    public static <T> JetEvent<T> jetEvent(@Nullable T payload, @Nonnull Object key, long timestamp) {
        if (payload == null) {
            return null;
        }
        assert key != null : "null key";
        return new JetEvent<>(payload, key, UNINITIALIZED_PARTITION_ID, timestamp);
    }

    /**
     * Creates a new {@code JetEvent} for tests where the key is null and
     * partitionId is 0. We don't need the key as it's a part of the payload
     * normally.
     *
     * TODO [viliam] rename to testJetEvent
     */
    @Nullable
    public static <T> JetEvent<T> jetEvent(@Nullable T payload, long timestamp) {
        if (payload == null) {
            return null;
        }
        return new JetEvent<>(payload, null, 0, timestamp);
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

    public Object key() {
        return key;
    }

    public int partitionId() {
        assert partitionId != UNINITIALIZED_PARTITION_ID : "uninitialized partition id";
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        if (partitionId != UNINITIALIZED_PARTITION_ID) {
            this.partitionId = partitionId;
            this.key = null;
        }
    }

    @Override
    public String toString() {
        return "JetEvent{payload=" + payload + ", key=" + key + ", partitionId=" + partitionId + ", timestamp=" + timestamp
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JetEvent<?> jetEvent = (JetEvent<?>) o;
        return timestamp == jetEvent.timestamp &&
                partitionId == jetEvent.partitionId &&
                payload.equals(jetEvent.payload) &&
                Objects.equals(key, jetEvent.key);
    }
}
