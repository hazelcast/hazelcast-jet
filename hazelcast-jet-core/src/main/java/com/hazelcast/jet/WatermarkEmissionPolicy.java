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

import javax.annotation.Nonnull;
import java.io.Serializable;

import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * A policy object that decides the progress of watermarks.
 */
@FunctionalInterface
public interface WatermarkEmissionPolicy extends Serializable {

    /**
     * Determines, based on last emitted watermark and current watermark, what
     * the next watermark should be.
     * <p>
     * It returns watermark value to emit. If returned value is less than
     * {@code currentWm}, such watermark must be emitted and this method called
     * again until it returns a value {@code >= currentWm}. It should not be
     * called again, if {@code currentWm} is not greater than the value last
     * returned.
     *
     * @param lastEmittedWm Last emitted watermark, Long.MIN_VALUE initially
     * @param currentWm Current wanna-be watermark value
     */
    long nextWatermark(long lastEmittedWm, long currentWm);

    /**
     * Returns a policy that allows emission of all possible watermarks (that
     * is every millisecond, if millisecond is the time unit).
     * <p>
     * It is useful primarily in testing scenarios or some specific cases where
     * it is known that no watermark throttling is needed.
     */
    @Nonnull
    static WatermarkEmissionPolicy emitAll() {
        return (lastEmittedWm, newWm) -> newWm;
    }

    /**
     * Returns a watermark emission policy that ensures that each emitted
     * watermark's value is at least {@code minStep} more than the previous
     * one. This is a general, scenario-agnostic throttling policy.
     */
    @Nonnull
    static WatermarkEmissionPolicy emitByMinStep(long minStep) {
        checkPositive(minStep, "minStep");
        return (lastEmittedWm, newWm) -> Math.max(newWm, lastEmittedWm + minStep);
    }

    /**
     * Returns a watermark emission policy that ensures that the emitted
     * watermarks are on the verge of a frame (at the 0th moment belonging to
     * the frame). It also ensures that there is a watermark for each frame
     * even if no event belongs to that frame.
     * <p>
     * This emission policy should be employed to drive a downstream processor
     * that computes a sliding/tumbling window
     * ({@link com.hazelcast.jet.processor.Processors#accumulateByFrame(
     *      com.hazelcast.jet.function.DistributedFunction,
     *      com.hazelcast.jet.function.DistributedToLongFunction,
     *      TimestampKind, WindowDefinition, AggregateOperation)
     * accumulateByFrame()} or
     * {@link com.hazelcast.jet.processor.Processors#aggregateToSlidingWindow(
     *      com.hazelcast.jet.function.DistributedFunction,
     *      com.hazelcast.jet.function.DistributedToLongFunction,
     *      TimestampKind, WindowDefinition, AggregateOperation)
     * aggregateToSlidingWindow()}).
     */
    @Nonnull
    static WatermarkEmissionPolicy emitByFrame(@Nonnull WindowDefinition wDef) {
        return (lastEmittedWm, newWm) ->
                lastEmittedWm == Long.MIN_VALUE
                        ? newWm % wDef.frameLength() == 0
                                ? newWm
                                : wDef.higherFrameTs(newWm)
                        : wDef.higherFrameTs(lastEmittedWm);
    }
}
