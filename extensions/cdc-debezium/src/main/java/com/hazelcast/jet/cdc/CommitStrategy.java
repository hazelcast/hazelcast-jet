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

import java.io.Serializable;

/**
 * Description of the strategy for confirming processed change record offsets to
 * databases backing CDC sources. Not all databases make use of this feedback,
 * but some do, for example, Postgres. Postgres' replication slots rely on
 * this feedback to clean up their internal data structures. Once the source
 * confirms an offset as processed, the replication slot will not be able to
 * resend data for that or older offsets.
 *
 * @since 4.5
 */
public interface CommitStrategy extends Serializable {

    /**
     * Returns whether the last batch should be committed. This method will be
     * called after each batch.
     * <p>
     * Implementations can always return {@code true} to immediately commit
     * each batch, or they can return {@code true} only when, for example, the
     * last batch confirmation was more than a certain time ago, thus producing
     * periodic commits.
     */
    boolean commitBatch();

    /**
     * Returns whether the offsets should be committed as a part of the current
     * snapshot. This method will be called for each snapshot. This is the
     * natural choice for commits since the last received offset is also saved
     * into the state snapshot and jobs are resumed from it.
     * <p>
     * Even though it makes the most sense, this kind of commit is not always
     * suitable, for example in jobs without a processing guarantee, which
     * never take any snapshots, so would never confirm any offsets, as
     * processed, for the replication slot. This can lead to resource
     * exhaustion on the DB side.
     */
    boolean commitOnSnapshot();
}
