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
 * Specifies what snapshot data to send to which processors upon snapshot
 * restoration.
 */
public enum SnapshotRestorePolicy {

    /**
     * Specifies that the snapshot data is partitioned and each processor will
     * get only the keys whose partitions it is responsible for. This is
     * analogous to sending the snapshot data over a partitioned-distributed
     * edge with the default partitioning strategy and the partitioning key
     * being the snapshot entry's key.
     * <p>
     * The partitioning of the data the processor receives over its inbound
     * edges must exactly align with the partitioning of the snapshot data.
     * Therefore all the processor's inbound edges must be distributed and
     * partitioned with the default strategy, and the partitioning key must be
     * the same as that used in the snapshot. Note that the above pertains only
     * to the edges that contribute to the snapshotted data. There may be other
     * edges whose data is processed statelessly and their partitioning doesn't
     * matter.
     */
    PARTITIONED,

    /**
     * All the snapshot data will be broadcast to all processor instances. This
     * is useful only for data global to all processors.
     */
    BROADCAST
}
