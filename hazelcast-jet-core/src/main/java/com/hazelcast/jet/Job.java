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

package com.hazelcast.jet;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.Pipeline;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * A Jet computation job created from a {@link DAG} or {@link Pipeline}.
 * Once submitted, Jet starts executing the job automatically.
 */
public interface Job {

    /**
     * Returns the ID of this job.
     *
     * @throws IllegalStateException if the job has not started yet, and thus has no id.
     */
    long getId();

    /**
     * Returns the configuration this job was submitted with. Changes made to the
     * returned config object will not have any affect.
     */
    @Nonnull
    JobConfig getConfig();

    /**
     * Returns the name of this job or {@code null} if no name was supplied.
     * <p>
     * Jobs can be named through {@link JobConfig#setName(String)} prior to submission.
     */
    @Nullable
    default String getName() {
        return getConfig().getName();
    }

    /**
     * Returns the time when the job was submitted to the cluster.
     * <p>
     * The time is assigned by reading {@code System.currentTimeMillis()} of
     * the master member that executes the job for the first time. It doesn't
     * change on restart.
     */
    long getSubmissionTime();

    /**
     * Returns the current status of this job.
     */
    @Nonnull
    JobStatus getStatus();

    /**
     * Gets the future associated with the job. The returned future is
     * not cancellable. To cancel the job, the {@link #cancel()} method
     * should be used.
     *
     * @throws IllegalStateException if the job has not started yet.
     */
    @Nonnull
    CompletableFuture<Void> getFuture();

    /**
     * Waits for the job to complete and throws an exception if the job
     * completes with an error. Does not return if the job gets suspended.
     * Never returns for streaming (unbounded) jobs unless they fail.
     * <p>
     * Shorthand for <code>job.getFuture().get()</code>.
     */
    default void join() {
        Util.uncheckRun(() -> getFuture().get());
    }

    /**
     * Gracefully stops the current execution and schedules a new execution
     * with the current member list of the Jet cluster. Can be called to
     * manually make use of added members, if {@linkplain
     * JobConfig#setAutoScaling auto scaling} is disabled. Only a running job
     * can be restarted; a suspended job must be {@linkplain #resume() resumed}.
     * <p>
     * Conceptually this call is equivalent to {@link #suspend()} & {@link
     * #resume()}.
     *
     * @throws IllegalStateException if the job is not running, for example it
     * has already completed, is not yet running, is already restarting,
     * suspended etc.
     */
    void restart();

    /**
     * Gracefully suspends the current execution of the job. The job's status
     * will become {@link JobStatus#SUSPENDED}. To resume the job, call {@link
     * #resume()}.
     * <p>
     * You can suspend a job even if it's not configured for {@linkplain
     * JobConfig#setProcessingGuarantee snapshotting}. Such a job will resume
     * with empty state, as if it has just been started.
     * <p>
     * This call just initiates the suspension process and doesn't wait for it
     * to complete. Suspension starts with creating a terminal state snapshot.
     * Should the terminal snapshot fail, the job will suspend anyway, but the
     * previous snapshot (if there was one) won't be deleted. When the job
     * resumes, its processing starts from the point of the last snapshot.
     * <p>
     * <strong>NOTE:</strong> if the cluster becomes unstable (a member leaves or
     * similar) while the job is in the process of being suspended, it may end up
     * getting immediately restarted. Call {@link #getStatus()} to find out and
     * possibly try to suspend again.
     *
     * @throws IllegalStateException if the job is not running
     */
    void suspend();

    /**
     * Resumes a {@linkplain #suspend stopped} job.
     */
    void resume();

    /**
     * Makes a request to cancel this job and returns. The job will complete
     * after its execution has stopped on all the nodes. If the job is
     * already suspended, Jet will delete its runtime state so it can't be
     * resumed.
     * <p>
     * <strong>NOTE:</strong> if the cluster becomes unstable (a member leaves or
     * similar) while the job is in the process of being cancelled, it may end up
     * getting restarted after the cluster has stabilized. Call {@link
     * #getStatus()} to find out and possibly try to cancel again.
     *
     * @throws IllegalStateException if the job is not running: is restarting,
     * completed, etc.
     */
    void cancel();
}
