/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.retry;

import com.hazelcast.jet.retry.impl.IntervalFunctions;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Function that can compute the wait length of each retry attempt. The input
 * is the number of the attempt, the output is the wain interval in milliseconds.
 *
 * @since 4.3
 */
@FunctionalInterface
public interface IntervalFunction extends Serializable {

    /**
     * Creates an {@code IntervalFunction} which returns a fixed interval in
     * milliseconds.
     */
    static IntervalFunction constant(long intervalValue, TimeUnit intervalUnit) {
        return constant(intervalUnit.toMillis(intervalValue));
    }

    /**
     * Creates an {@code IntervalFunction} which returns a fixed interval in
     * milliseconds.
     */
    static IntervalFunction constant(long intervalMs) {
        return IntervalFunctions.constant(intervalMs);
    }

    /**
     * Creates an {@code IntervalFunction} which starts from the specified wait
     * interval, on the first attempt, and for each subsequent attempt uses
     * a longer interval, equal to the previous wait duration multiplied by the
     * provided scaling factor.
     */
    static IntervalFunction exponentialBackoff(long intervalValue, TimeUnit intervalUnit, double multiplier) {
        return exponentialBackoff(intervalUnit.toMillis(intervalValue), multiplier);
    }

    /**
     * Creates an {@code IntervalFunction} which starts from the specified wait
     * interval, on the first attempt, and for each subsequent attempt uses
     * a longer interval, equal to the previous wait duration multiplied by the
     * provided scaling factor.
     */
    static IntervalFunction exponentialBackoff(long intervalMillis, double multiplier) {
        return IntervalFunctions.exponentialBackoff(intervalMillis, multiplier);
    }

    /**
     * Returns the wait time required before the specified attempt. In this
     * context attempt no. 1 means the first attempt, so it's not an index and
     * zero is not an allowed input.
     */
    long waitAfterAttempt(int attempt);
}
