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

import javax.annotation.CheckReturnValue;

/**
 * Interface for saving snapshot to storage.
 */
public interface SnapshotStorage {

    /**
     * Store one key-value pair. The key must be globally unique for this
     * vertex.
     * <p>
     * If the {@code key} implements {@link com.hazelcast.core.PartitionAware}
     * then it will be used to choose the target partition, instead of the
     * whole key.
     *
     * @return {@code true}, if the item was accepted by the queue. If {@code
     * false} is returned the call should be retried later <b>with the same key
     * and value</b>. For non-cooperative processor a blocking implementation
     * is provided, that always returns {@code true}.
     *
     * @throws IllegalArgumentException If a duplicate key is stored.
     * Implementation is allowed to throw it, but not required.
     */
    @CheckReturnValue
    boolean offer(Object key, Object value);
}
