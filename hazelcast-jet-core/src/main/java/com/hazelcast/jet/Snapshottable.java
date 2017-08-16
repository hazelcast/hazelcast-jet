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

/**
 * An interface to be implemented by a {@link Processor} that wants to do
 * snapshots and be able to restore the state after restart or failure.
 */
public interface Snapshottable {

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
     * method must also be cooperative.
     * <p>
     * If {@code false} is returned, the method will be called again before any
     * other methods are called.
     * <p>
     * After {@link Processor#complete()} returned {@code true}, this method
     * won't be called anymore.
     */
    boolean saveSnapshot();

    /**
     * Restore processor's state from an inbox with snapshotted state. Items
     * are of type {@code Map.Entry<Object, Object>}. The inbox contains just
     * one batch of items, method will be called multiple times if needed. If
     * there is no snapshot to restore, method won't be called at all, even
     * though the processors is stateful.
     * <p>
     * Processor is allowed to put items to Outbox during this call.
     */
    void restoreSnapshot(@Nonnull Inbox inbox);

    /**
     * Called after all keys have been restored using {@link
     * #restoreSnapshot(Inbox)}.
     */
    default boolean finishSnapshotRestore() {
        return true;
    }
}
