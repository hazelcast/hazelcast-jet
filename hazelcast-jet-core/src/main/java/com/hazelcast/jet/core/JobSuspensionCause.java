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

package com.hazelcast.jet.core;

import com.hazelcast.jet.config.JobConfig;

import javax.annotation.Nullable;

/**
 * Description of the cause that has lead to the job being suspended.
 * <p>
 * One reason for the job to be suspended is the user explicitly requesting it.
 * Another reason is encountering an error during execution, while being
 * configured to get suspended in such cases, instead of just failing. See
 * {@link JobConfig#setSuspendOnFailure(boolean)}.
 * <p>
 * When the job has been suspended due to an error, then the cause of that
 * error is provided.
 */
public interface JobSuspensionCause {

    /**
     * True if the job has been suspended due to an explicit request from the
     * user.
     */
    boolean requestedByUser();

    /**
     * True if the job has been suspended due to encountering an error during
     * execution.
     */
    boolean dueToError();

    /**
     * Provides the error that has lead to the suspension of the job, or returns
     * null if the cause of the suspension is different.
     */
    @Nullable
    Throwable errorCause();

}
