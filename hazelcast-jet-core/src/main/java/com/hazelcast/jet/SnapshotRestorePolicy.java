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

/**
 * Snapshot restore policies.
 */
public enum SnapshotRestorePolicy {

    /**
     * Specifies that only part of the state will be restored to each processor
     * instance.
     * <p>
     * The processor must be preceded with an edge which satisfies these
     * conditions:<ol>
     * <li>is {@link Edge#distributed() distributed}
     * <li>is {@link Edge#partitioned(com.hazelcast.jet.function.DistributedFunction)
     * partitioned}
     * <li>is partitioned by the same key as is used in the snapshot.
     * <li>use the default partitioner
     * </ol>
     *
     * If there are multiple inbound edges, all must satisfy the conditions. If
     * some condition is not met then state for some key might be restored to
     * one processor instance and items for that key will be delivered to
     * another. This situation is currently not detected and might go
     * unnoticed, producing incorrect results.
     *<p>
     * With this type of state saving and restoring of a snapshot is done
     * locally with two exceptions:<ul>
     *
     * <li>partition migration took place since the job started: job doesn't
     * migrate partitions, but IMap does (unless disabled). In this case, some
     * partitions of the snapshot will be stored remotely.
     *
     * <li>backup copies are always stored remotely, obviously.
     * </ul>
     */
    PARTITIONED,

    /**
     * Entire snapshot will be restored to all processor instances. Use this
     * option if partitions of keys in the snapshot don't match the Hazelcast
     * partitions of this processor.
     */
    BROADCAST
}
