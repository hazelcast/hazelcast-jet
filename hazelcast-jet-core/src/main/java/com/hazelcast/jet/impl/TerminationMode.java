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

package com.hazelcast.jet.impl;

public enum TerminationMode {

    RESTART_GRACEFUL(true, true, false),
    RESTART_FORCEFUL(false, true, false),
    SUSPEND_GRACEFUL(true, false, false),
    SUSPEND_FORCEFUL(false, false, false),
    CANCEL(false, false, true);

    private final boolean stopWithSnapshot;
    private final boolean restart;
    private final boolean deleteData;

    TerminationMode(boolean stopWithSnapshot, boolean restart, boolean deleteData) {
        this.stopWithSnapshot = stopWithSnapshot;
        this.restart = restart;
        this.deleteData = deleteData;
    }

    /**
     * If true, the job should be terminated with a snapshot. If false, it
     * should be interrupted.
     */
    public boolean isStopWithSnapshot() {
        return stopWithSnapshot;
    }

    /**
     * If true, the job should restart after termination.
     */
    public boolean isRestart() {
        return restart;
    }

    /**
     * If true, job resources and snapshots should be deleted after
     * termination.
     */
    public boolean isDeleteData() {
        return deleteData;
    }
}
