/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.pipeline;

/**
 * Javadoc pending.
 */
public final class JetEventImpl<T> implements JetEvent<T> {
    private final long timestamp;
    private final T payload;

    private JetEventImpl(long timestamp, T payload) {
        this.timestamp = timestamp;
        this.payload = payload;
    }

    public static <T> JetEvent<T> jetEvent(T payload, long timestamp) {
        return new JetEventImpl<>(timestamp, payload);
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public T payload() {
        return payload;
    }

    @Override
    public String toString() {
        return String.format("%,d %s", timestamp, payload);
    }
}
