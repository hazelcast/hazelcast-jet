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

public enum SnapshotRestorePolicy {

    /**
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
     */
    PARTITIONED,

    /**
     * Entire snapshot will be restored to all processor instances. To
     * limit the traffic, use {@link #getSnapshotPredicate()}. Use this
     * option, if partitions of keys in the snapshot don't match the
     * Hazelcast partitions of this processor.
     */
    BROADCAST
}
