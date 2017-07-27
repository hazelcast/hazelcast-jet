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

package com.hazelcast.jet.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.coordination.JobRepository;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;

import javax.annotation.Nonnull;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;

public abstract class AbstractJobImpl implements Job {

    private final JobRepository jobRepository;
    private final CompletableFuture<Void> future = new CompletableFuture<>();
    private Long jobId;
    private DAG dag;
    private JobConfig config;

    AbstractJobImpl(JetInstance jetInstance, DAG dag, JobConfig config) {
        this.jobRepository = new JobRepository(jetInstance);
        this.jobId = null;
        this.dag = dag;
        this.config = config;
    }

    @Nonnull
    @Override
    public JobConfig getConfig() {
        return config;
    }

    @Nonnull
    @Override
    public DAG getDAG() {
        return dag;
    }

    @Nonnull
    @Override
    public Future<Void> getFuture() {
        if (jobId == null) {
            throw new IllegalStateException("Job not yet started, use execute()");
        }

        return future;
    }

    protected abstract ICompletableFuture<Void> sendJoinJobOp();

    @Override
    public long getJobId() {
        if (jobId == null) {
            throw new IllegalStateException("ID not yet assigned");
        }
        return jobId;
    }

    void initialize() {
        if (jobId != null) {
            throw new IllegalStateException("Job already started");
        }

        jobId = jobRepository.newId();
        jobRepository.uploadJobResources(jobId, config);
        jobRepository.newJobRecord(jobId, dag);

        ICompletableFuture<Void> invocationFuture = sendJoinJobOp();
        JobCallback callback = new JobCallback(invocationFuture);
        invocationFuture.andThen(callback);
        future.whenComplete((aVoid, throwable) -> {
            if (throwable instanceof CancellationException) {
                callback.cancel();
            }
        });
    }

    private class JobCallback implements ExecutionCallback<Void> {

        private volatile ICompletableFuture<Void> invocationFuture;

        JobCallback(ICompletableFuture<Void> invocationFuture) {
            this.invocationFuture = invocationFuture;
        }

        @Override
        public void onResponse(Void response) {
            future.complete(response);
        }

        @Override
        public void onFailure(Throwable t) {
            if (isRestartable(t)) {
                synchronized (this) {
                    try {
                        ICompletableFuture<Void> invocationFuture = sendJoinJobOp();
                        this.invocationFuture = invocationFuture;
                        invocationFuture.andThen(this);
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                }
            } else {
                future.completeExceptionally(t);
            }
        }

        private boolean isRestartable(Throwable t) {
            Throwable cause = peel(t);
            return cause  instanceof MemberLeftException
                    || cause instanceof TargetDisconnectedException
                    || cause instanceof TargetNotMemberException;
        }

        public synchronized void cancel() {
            invocationFuture.cancel(true);
        }

    }
}
