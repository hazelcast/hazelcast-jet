/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.jet.core.SlidingWindowPolicy;

import static com.hazelcast.jet.WindowDefinition.WindowKind.TUMBLING;
import static com.hazelcast.jet.core.SlidingWindowPolicy.tumblingWinPolicy;

/**
 * Javadoc pending.
 */
public class TumblingWindowDef extends SlidingWindowDef {

    TumblingWindowDef(long windowSize) {
        super(windowSize, 1);
    }

    @Override
    public WindowKind kind() {
        return TUMBLING;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TumblingWindowDef downcast() {
        return this;
    }

    /**
     * Returns the length of the window (the size of the timestamp range it
     * covers).
     */
    @Override
    public long windowSize() {
        return super.windowSize();
    }

    /**
     * A tumbling window advances in steps equal to its size, therefore this
     * method always returns 1.
     */
    public long frameSize() {
        return super.frameSize();
    }

    @Override
    public SlidingWindowPolicy toSlidingWindowPolicy() {
        return tumblingWinPolicy(windowSize());
    }
}
