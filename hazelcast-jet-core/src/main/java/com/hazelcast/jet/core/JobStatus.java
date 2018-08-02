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

package com.hazelcast.jet.core;

/**
 * Represents current status of the job from the perspective of the job coordinator.
 */
public enum JobStatus {

    /**
     * The job is submitted but not started yet. This state is used also when
     * the job was terminated for some reason but before it is started again
     * (for example, if a member is shut down, job is terminated, but not
     * started again until the migrations are complete).
     */
    NOT_RUNNING,

    /**
     * The job is in the initialization phase on new coordinator, in which it
     * starts the execution.
     */
    STARTING,

    /**
     * The job is currently running.
     */
    RUNNING,

    /**
     * The job was suspended and it can be manually resumed.
     */
    SUSPENDED,

    /**
     * The job is currently being completed.
     */
    COMPLETING,

    /**
     * The job is failed with an exception.
     */
    FAILED,

    /**
     * The job is completed successfully.
     */
    COMPLETED

}
