/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.config;

public enum ElasticityMode {

    /**
     * <em>On member leave:</em> job will be suspended.
     *
     * <p><em>On member add:</em> nothing happens, new member won't be used
     */
    DISABLED,

    /**
     * <em>On member leave:</em> job will be restarted from last snapshot. If
     * snapshotting is disabled, job will be restarted from scratch.
     *
     * <p><em>On member add:</em> nothing happens, new member won't be used
     */
    RESTART_ON_FAILURE,

    /**
     * <em>On member leave:</em> job will be restarted from last snapshot. If
     * snapshotting is disabled, job will be restarted from scratch.
     *
     * <p><em>On member add:</em> job will be shut down gracefully and
     * restarted. If snapshotting is disabled, job will be restarted from
     * scratch forcefully.
     */
    ELASTIC_GRACEFUL,

    /**
     * <em>On member leave:</em> job will be restarted from last snapshot. If
     * snapshotting is disabled, job will be restarted from scratch.
     *
     * <p><em>On member add:</em> job will be restarted forcefully from last
     * snapshot. If snapshotting is disabled, job will be restarted from
     * scratch.
     */
    ELASTIC_FORCEFUL,

    /**
     * <em>On member leave:</em> job will be restarted from last snapshot. If
     * snapshotting is disabled, job will be restarted from scratch.
     *
     * <p><em>On member add:</em> job will be shut down gracefully after next
     * scheduled snapshot and restarted. If snapshotting is disabled, same as
     * {@link #ELASTIC_FORCEFUL}.
     */
    ELASTIC_NEXT_SNAPSHOT
}
