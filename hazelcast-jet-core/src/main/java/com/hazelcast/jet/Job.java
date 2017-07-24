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

import javax.annotation.Nonnull;
import java.util.concurrent.Future;

/**
 * A Jet computation job created from a {@link DAG}, ready to be executed.
 */
public interface Job {
    /**
     * Executes the job.
     *
     * @return a future that can be inspected for job completion status and
     * cancelled to prematurely end the job.
     *
     * @throws IllegalStateException If the job was already started.
     */
    @Nonnull
    Future<Void> execute();

    /**
     * Gets the future associated with the job, used to control the job.
     *
     * @throws IllegalStateException If the job was not started yet.
     */
    @Nonnull
    Future<Void> getFuture();

    /**
     * Return the ID of this job.
     *
     * @throws IllegalStateException If the job was not started yet, and thus
     * has no job id.
     */
    long getJobId();
}
