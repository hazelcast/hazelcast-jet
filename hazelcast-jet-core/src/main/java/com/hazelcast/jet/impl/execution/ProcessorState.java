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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Snapshottable;

enum ProcessorState {
    /**
     * Check, if new snapshot is requested and wait to accept the {@link
     * SnapshotStartBarrier} by the queue.
     */
    START_SNAPSHOT,

    /**
     * Doing calls to {@link Snapshottable#saveSnapshot()} until
     * it returns true.
     */
    DO_SNAPSHOT,

    /**
     * Waiting to accept the {@link SnapshotBarrier} by the queue.
     */
    FORWARD_SNAPSHOT_BARRIER,

    /**
     * Doing calls to {@link Processor#tryProcess()} until it returns true.
     */
    NULLARY_PROCESS,

    /**
     * Doing calls to {@link Processor#process(int, Inbox)} until the inbox is
     * empty.
     */
    PROCESS_INBOX,

    /**
     * Doing calls to {@link Processor#complete()} until it returns true
     */
    COMPLETE,
    /**
     * Waiting until outbox accepts DONE_ITEM.
     */
    FORWARD_DONE_ITEM,

    /**
     * waiting to flush the outbox. This is a terminal state.
     */
    PROCESSOR_COMPLETED
}
