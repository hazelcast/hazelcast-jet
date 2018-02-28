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

package com.hazelcast.jet.pipeline;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.pipeline.WindowDefinition.WindowKind.SESSION;

/**
 * Represents the definition of a {@link
 * com.hazelcast.jet.pipeline.WindowDefinition.WindowKind#SESSION
 * session} window.
 *
 * @param <T> type of the stream item
 */
public class SessionWindowDef<T> implements WindowDefinition {
    private static final int MAX_FRAME_RATE = 100;
    private static final int MIN_WMS_PER_SESSION = 100;
    private final long sessionTimeout;

    SessionWindowDef(long sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    @Nonnull @Override
    public WindowKind kind() {
        return SESSION;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public SessionWindowDef<T> downcast() {
        return this;
    }

    @Override
    public long watermarkFrameSize() {
        return Math.min(MAX_FRAME_RATE, sessionTimeout / MIN_WMS_PER_SESSION);
    }

    /**
     * Returns the session timeout, which is the largest difference in the
     * timestamps of any two consecutive events in the session window.
     */
    public long sessionTimeout() {
        return sessionTimeout;
    }
}
