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

import com.hazelcast.query.Predicate;

import javax.annotation.Nullable;

/**
 * An interface to be implemented by a {@link Processor} that wants to do
 * snapshots and be able to restore the state after restart or failure.
 */
public interface Snapshottable {

    /**
     * Returns, if the processors uses partitioned state.
     * <ul>
     *
     * <li><b>Partitioned state</b><br/>
     * The processor must be preceded with a {@link Edge#distributed()
     * distributed} and {@link
     * Edge#partitioned(com.hazelcast.jet.function.DistributedFunction)
     * partitioned} edge using default partitioner and partitioned by the
     * same key as is used in the snapshot.
     * <p>
     * Correct keys will be restored to each processor and saving and
     * restoring the snapshot will be done locally (except if the partitioning
     * in the HZ map changed since the job started and except for the backup
     * copies of the snapshot).
     *
     * <li><b>Broadcast state</b><br/>
     * Entire snapshot will be restored to all processor instances. To
     * limit the traffic, use {@link #getSnapshotPredicate()}. Use this
     * option, if partitions of keys in the snapshot don't match the
     * Hazelcast partitions of this processor.
     *</ul>
     */
    boolean isPartitionedSnapshot();

    /**
     * Store the state to the snapshot. Return {@code true} if done, or {@code
     * false} if the method should be called again. Method is allowed to add
     * items to outbox during this call.
     * <p>
     * The method will never be called, if the inbox is not empty after the
     * {@link Processor#process(int, Inbox)} method returns.
     * After the inbox is done (this includes source processors), the method
     * can be called anytime between {@link Processor#complete()} calls. If a
     * processor never returns from {@link Processor#complete()} (which is
     * allowed for non-cooperative processors), method will never be called.
     * <p>
     * Snapshot method will always be called on the same thread as other
     * processing methods, so no synchronization is necessary.
     * <p>
     * If the processor {@link Processor#isCooperative() is cooperative}, this
     * method must also be cooperative. Using {@code storage} satisfies this
     * condition.
     * <p>
     * If {@code false} is returned, the method will be called again before any
     * other methods are called.
     * <p>
     * After {@link Processor#complete()} returned {@code true}, this method
     * won't be called anymore.
     */
    boolean saveSnapshot(SnapshotStorage storage);

    /**
     * Apply a key from a snapshot to processor’s internal state.
     */
    void restoreSnapshotKey(Object key, Object value);

    /**
     * Returns the predicate to use when restoring snapshot. Only used if
     * {@link #isPartitionedSnapshot()} returns {@code false}.
     */
    @Nullable
    default Predicate getSnapshotPredicate() {
        return null;
    }

    /**
     * Called after all keys have been restored using {@link
     * #restoreSnapshotKey(Object, Object)}.
     */
    default void finishSnapshotRestore() {
    }

    /**
     * Clear entire state, that might have been restored using {@link
     * #restoreSnapshotKey(Object, Object)}.
     * <p>
     * This will be used, if partition migration took place during restoring,
     * in which case the process has to be started over.
     */
    default void clearState() {
    }

    public enum SnapshotResult {
        /**
         * Saving snapshot is not finished and the {@link
         * #saveSnapshot(SnapshotStorage)} method should be called again.
         */
        NOT_DONE,

        /**
         * Saving snapshot can continue in asynchronous way.
         */
        DONE_ASYNC,
        DONE;
    }
}
