/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.core.Outbox;

public interface OutboxInternal extends Outbox {

    /**
     * Resets the counter that prevents adding more than {@code batchSize}
     * items until this method is called again. Note that the counter may
     * jump to the "full" state even before the outbox accepted that many items.
     */
    void reset();

    /**
     * Blocks the outbox so it allows the caller only to offer the current
     * unfinished item. If there is no unfinished item, the outbox will reject
     * all items until you call {@link #unblock()}.
     */
    void block();

    /**
     * Removes the effect of a previous {@link #block()} call (if any).
     */
    void unblock();

    /**
     * Returns the timestamp of the last forwarded watermark.
     * <p>
     * If there was no watermark added, it returns {@code Long.MIN_VALUE}. Can
     * be called from a concurrent thread.
     */
    long lastForwardedWm();

    /**
     * Return the snapshot data size in bytes that has been written out (so
     * far) through this outbox (so keys and values).
     * <p>
     * The size value is not strictly accurate in the sense that it's not the
     * exact number of bytes that will end up in persistent storage. It's
     * just the useful size of snapshot data (number of bytes of the
     * serialized form) and does not include any overhead incurred by the
     * concrete persistent storage technology being used.
     * <p>
     * Should still be useful, should give an accurate estimate of the
     * data volumes going out towards persistent storage.
     */
    long snapshotSize();

}
