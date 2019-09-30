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

package com.hazelcast.jet.impl;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetExportSnapshotCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobConfigCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobMetricsCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobStatusCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobStatusCodec.ResponseParameters;
import com.hazelcast.client.impl.protocol.codec.JetGetJobSubmissionTimeCodec;
import com.hazelcast.client.impl.protocol.codec.JetJoinSubmittedJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetResumeJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetSubmitJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetTerminateJobCodec;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.jet.impl.JobMetricsUtil.toJobMetrics;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

/**
 * {@link Job} proxy on client.
 */
public class ClientJobProxy extends AbstractJobProxy<JetClientInstanceImpl> {

    ClientJobProxy(JetClientInstanceImpl client, long jobId) {
        super(client, jobId);
    }

    ClientJobProxy(JetClientInstanceImpl client, long jobId, DAG dag, JobConfig config) {
        super(client, jobId, dag, config);
    }

    @Nonnull @Override
    public JobStatus getStatus() {
        ClientMessage request = JetGetJobStatusCodec.encodeRequest(getId());
        try {
            ClientMessage response = invocation(request, masterAddress()).invoke().get();
            ResponseParameters parameters = JetGetJobStatusCodec.decodeResponse(response);
            return JobStatus.values()[parameters.response];
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Nonnull @Override
    public JobMetrics getMetrics() {
        ClientMessage request = JetGetJobMetricsCodec.encodeRequest(getId());
        try {
            ClientMessage response = invocation(request, masterAddress()).invoke().get();
            JetGetJobMetricsCodec.ResponseParameters parameters = JetGetJobMetricsCodec.decodeResponse(response);
            return toJobMetrics(serializationService().toObject(parameters.response));
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    protected ICompletableFuture<Void> invokeSubmitJob(Data dag, JobConfig config) {
        Data configData = serializationService().toData(config);
        ClientMessage request = JetSubmitJobCodec.encodeRequest(getId(), dag, configData);
        return new CancellableFuture<>(invocation(request, masterAddress()).invoke());
    }

    @Override
    protected ICompletableFuture<Void> invokeJoinJob() {
        ClientMessage request = JetJoinSubmittedJobCodec.encodeRequest(getId());
        ClientInvocation invocation = invocation(request, masterAddress());
        // this invocation should never time out, as the job may be running for a long time
        invocation.setInvocationTimeoutMillis(Long.MAX_VALUE); // 0 is not supported
        return new CancellableFuture<>(invocation.invoke());
    }

    @Override
    protected ICompletableFuture<Void> invokeTerminateJob(TerminationMode mode) {
        ClientMessage request = JetTerminateJobCodec.encodeRequest(getId(), mode.ordinal());
        return new CancellableFuture<>(invocation(request, masterAddress()).invoke());
    }

    @Override
    public void resume() {
        ClientMessage request = JetResumeJobCodec.encodeRequest(getId());
        try {
            new CancellableFuture<>(invocation(request, masterAddress()).invoke()).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    public JobStateSnapshot cancelAndExportSnapshot(String name) {
        return doExportSnapshot(name, true);
    }

    @Override
    public JobStateSnapshot exportSnapshot(String name) {
        return doExportSnapshot(name, false);
    }

    private JobStateSnapshot doExportSnapshot(String name, boolean cancelJob) {
        ClientMessage request = JetExportSnapshotCodec.encodeRequest(getId(), name, cancelJob);
        try {
            new CancellableFuture<>(invocation(request, masterAddress()).invoke()).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
        return container().getJobStateSnapshot(name);
    }

    @Override
    protected long doGetJobSubmissionTime() {
        ClientMessage request = JetGetJobSubmissionTimeCodec.encodeRequest(getId());
        try {
            ClientMessage response = invocation(request, masterAddress()).invoke().get();
            return JetGetJobSubmissionTimeCodec.decodeResponse(response).response;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    protected JobConfig doGetJobConfig() {
        ClientMessage request = JetGetJobConfigCodec.encodeRequest(getId());
        try {
            ClientMessage response = invocation(request, masterAddress()).invoke().get();
            Data data = JetGetJobConfigCodec.decodeResponse(response).response;
            return serializationService().toObject(data);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    protected Address masterAddress() {
        Optional<Member> first = container().getCluster().getMembers().stream().findFirst();
        return first.orElseThrow(() -> new IllegalStateException("No members found in cluster")).getAddress();
    }

    @Override
    protected SerializationService serializationService() {
        return container().getHazelcastClient().getSerializationService();
    }

    @Override
    protected LoggingService loggingService() {
        return container().getHazelcastClient().getLoggingService();
    }

    private ClientInvocation invocation(ClientMessage request, Address invocationAddr) {
        return new ClientInvocation(
                container().getHazelcastClient(), request, "jobId=" + getIdString(), invocationAddr
        );
    }

    /**
     * Decorator for execution future which makes it cancellable
     */
    private static class CancellableFuture<T> implements ICompletableFuture<Void> {

        private final ICompletableFuture<T> future;

        CancellableFuture(ICompletableFuture<T> future) {
            this.future = future;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return future.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            future.get();
            return null;
        }

        @Override
        public Void get(long timeout, @Nonnull TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            future.get(timeout, unit);
            return null;
        }

        @Override
        public void andThen(ExecutionCallback<Void> callback) {
            future.andThen(new ExecutionCallback<T>() {
                @Override
                public void onResponse(T response) {
                    callback.onResponse(null);
                }

                @Override
                public void onFailure(Throwable t) {
                    callback.onFailure(t);
                }
            });
        }

        @Override
        public void andThen(ExecutionCallback<Void> callback, Executor executor) {
            future.andThen(new ExecutionCallback<T>() {
                @Override
                public void onResponse(T response) {
                    callback.onResponse(null);
                }

                @Override
                public void onFailure(Throwable t) {
                    callback.onFailure(t);
                }
            }, executor);
        }
    }
}
