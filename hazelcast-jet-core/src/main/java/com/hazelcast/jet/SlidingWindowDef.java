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

import static com.hazelcast.jet.WindowDefinition.WindowKind.SLIDING;
import static com.hazelcast.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Javadoc pending.
 */
public class SlidingWindowDef implements WindowDefinition {
    private final long frameSize;
    private final long windowSize;

    SlidingWindowDef(long frameSize, long framesPerWindow) {
        checkPositive(frameSize, "frameLength must be positive");
        checkPositive(framesPerWindow, "framesPerWindow must be positive");
        this.frameSize = frameSize;
        this.windowSize = frameSize * framesPerWindow;
    }

    @Override
    public WindowKind kind() {
        return SLIDING;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SlidingWindowDef downcast() {
        return this;
    }

    /**
     * Returns the length of the window (the size of the timestamp range it
     * covers). It is an integer multiple of {@link #frameSize()}.
     */
    public long windowSize() {
        return windowSize;
    }

    /**
     * Returns the length of the frame (equal to the sliding step).
     */
    public long frameSize() {
        return frameSize;
    }

    public SlidingWindowPolicy toSlidingWindowPolicy() {
        return slidingWinPolicy(windowSize, frameSize);
    }
}
