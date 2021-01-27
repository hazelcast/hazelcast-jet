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

package com.hazelcast.jet.cdc;

import java.util.concurrent.TimeUnit;

/**
 * Collection of factory methods for creating the most frequently used
 * {@link CommitStrategy CommitStrategies}.
 *
 * @since 4.5
 */
public final class CommitStrategies {

    private CommitStrategies() {
    }

    /**
     * Never commit the latest processed change record offset.
     */
    public static CommitStrategy never() {
        return new FixedStrategy(false, false);
    }

    /**
     * Commit the latest processed change record offset after each record batch.
     */
    public static CommitStrategy always() {
        return new FixedStrategy(true, false);
    }

    /**
     * Commit the latest processed change record offset whenever saving a Jet
     * snapshot.
     * <p>
     * <strong>NOTE:</strong> only jobs with processing guarantees enabled
     * do save Jet snapshots.
     */
    public static CommitStrategy onSnapshot() {
        return new FixedStrategy(false, true);
    }

    /**
     * Commit latest processed change record offset if last such commit happened
     * more than the specified time ago.
     * <p>
     * We check if the period passed in two cases:<ul>
     *     <li>each time a new record batch is processed
     *     <li>when a state snapshot is taken
     * </ul>
     *
     * Ideally, the specified period should be a multiple of the Jet snapshot
     * interval, and then it will always be strictly respected.
     */
    public static CommitStrategy periodically(TimeUnit unit, long period) {
        return new PeriodicStrategy(TimeUnit.NANOSECONDS.convert(period, unit));
    }

    private static class FixedStrategy implements CommitStrategy {

        private final boolean commitBatch;
        private final boolean commitOnSnapshot;

        FixedStrategy(boolean commitBatch, boolean commitOnSnapshot) {
            this.commitBatch = commitBatch;
            this.commitOnSnapshot = commitOnSnapshot;
        }

        @Override
        public boolean commitBatch() {
            return commitBatch;
        }

        @Override
        public boolean commitOnSnapshot() {
            return commitOnSnapshot;
        }
    }

    private static class PeriodicStrategy implements CommitStrategy {

        private final long periodNs;
        private long lastNs = System.nanoTime();

        PeriodicStrategy(long periodNs) {
            this.periodNs = periodNs;
        }

        @Override
        public boolean commitBatch() {
            return commit();
        }

        @Override
        public boolean commitOnSnapshot() {
            return commit();
        }

        private boolean commit() {
            long currentNs = System.nanoTime();
            if (currentNs > lastNs + periodNs) {
                lastNs = currentNs;
                return true;
            } else {
                return false;
            }
        }
    }
}
