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
     * Waits for the job to complete and throws exception if job is completed
     * with an error. Does not return if the job is suspended. Never returns
     * for streaming (unbounded) jobs unless they fail.
     *
     * <p>Shorthand for <code>job.getFuture().get()</code>.
     */
    default void join() {
        Util.uncheckRun(() -> getFuture().get());
    }

    /**
     * Gracefully stops the current execution and schedules a new execution
     * with the current member list of the Jet cluster. Can be called to
     * manually make use of added members.
     *
     * <p>Conceptually it's equivalent to {@link #suspend()} & {@link
     * #resume()}.
     *
     * <p>You cannot restart a suspended job, to do that, call {@link
     * #resume()}.
     *
     * @throws IllegalStateException if the job is not running, for example it
     * has been already completed, is not yet running, is already restarting,
     * suspended etc.
     */
    void restart();

    /**
     * Gracefully suspend the current execution. Job status will become {@link
     * JobStatus#SUSPENDED}. To resume the job, call {@link #resume()}.
     *
     * <p>If the job does not do {@linkplain JobConfig#setProcessingGuarantee
     * state snapshots}, it will be suspended anyway. When resumed, it will
     * start with an empty state.
     *
     * <p>This call returns quickly and a suspension process is initiated in
     * the cluster. This process starts with creating a terminal state
     * snapshot. Should the terminal snapshot fail, the job will suspend
     * anyway, but the previous snapshot (if there was one) won't be deleted.
     * When the job is resumed, a reprocessing since the last state snapshot
     * was taken will take place. It can also happen that if a restartable
     * exception happens concurrently to suspension process (such as a member
     * leaving), the job might not suspend but restart. Call the {@link
     * #getStatus()} to find out and possibly suspend again.
     *
     * <em>Note:</em> If the coordinator fails before the suspend is done, it
     * might happen that even though the call was successful, the job will not
     * suspend, but restart.
     *
     * @throws IllegalStateException if the job is not running
     */
    void suspend();

    /**
     * Resumes a {@linkplain #suspend stopped} job.
     */
    void resume();

    /**
     * Attempts to cancel execution of this job. The job will be completed
     * after the job has been stopped on all the nodes.
     *
     * <p>You can also cancel a {@linkplain #suspend() suspended} job, this
     * will cause the deletion of job resources.
     *
     * <p>Starting from version 0.6, <code>job.getFuture().cancel()</code>
     * fails with an exception.
     *
     * <em>Note:</em> If the coordinator fails before the cancellation is done,
     * it might happen that even though the call was successful, the job will
     * not cancel, but restart and continue.

     * @throws IllegalStateException if the job is not running: is restarting,
     * completed...
     */
    void cancel();
}
