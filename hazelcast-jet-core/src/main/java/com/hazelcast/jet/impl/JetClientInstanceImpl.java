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

package com.hazelcast.jet.impl;

import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientGetDistributedObjectsCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.client.protocol.codec.JetExistsDistributedObjectCodec;
import com.hazelcast.jet.impl.client.protocol.codec.JetGetJobIdsByNameCodec;
import com.hazelcast.jet.impl.client.protocol.codec.JetGetJobIdsCodec;
import com.hazelcast.jet.impl.client.protocol.codec.JetGetJobSummaryListCodec;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.stream.Collectors.toList;

/**
 * Client-side {@code JetInstance} implementation
 */
public class JetClientInstanceImpl extends AbstractJetInstance {

    private final HazelcastClientInstanceImpl client;
    private final SerializationService serializationService;

    public JetClientInstanceImpl(HazelcastClientInstanceImpl hazelcastInstance) {
        super(hazelcastInstance);
        this.client = hazelcastInstance;
        this.serializationService = client.getSerializationService();

        ExceptionUtil.registerJetExceptions(hazelcastInstance.getClientExceptionFactory());
    }

    @Nonnull
    @Override
    public JetConfig getConfig() {
        throw new UnsupportedOperationException("Jet Configuration is not available on the client");
    }

    @Nonnull
    @Override
    public List<Job> getJobs() {
        return invokeRequestOnMasterAndDecodeResponse(JetGetJobIdsCodec.encodeRequest(), resp -> {
            List<Long> jobs = JetGetJobIdsCodec.decodeResponse(resp).response;
            return jobs.stream().map(jobId -> new ClientJobProxy(this, jobId)).collect(toList());
        });
    }

    /**
     * Returns a list of jobs and a summary of their details.
     */
    @Nonnull
    public List<JobSummary> getJobSummaryList() {
        return invokeRequestOnMasterAndDecodeResponse(JetGetJobSummaryListCodec.encodeRequest(),
                response -> JetGetJobSummaryListCodec.decodeResponse(response).response);
    }

    @Nonnull
    public HazelcastClientInstanceImpl getHazelcastClient() {
        return client;
    }

    @Override
    public boolean existsDistributedObject(@Nonnull String serviceName, @Nonnull String objectName) {
        return invokeRequestOnAnyMemberAndDecodeResponse(
                JetExistsDistributedObjectCodec.encodeRequest(serviceName, objectName),
                response -> JetExistsDistributedObjectCodec.decodeResponse(response).response
        );
    }

    public List<DistributedObjectInfo> getDistributedObjects() {
        return invokeRequestOnAnyMemberAndDecodeResponse(
                ClientGetDistributedObjectsCodec.encodeRequest(),
                response -> ClientGetDistributedObjectsCodec.decodeResponse(response).response
        );
    }

    @Override
    public List<Long> getJobIdsByName(String name) {
        return invokeRequestOnMasterAndDecodeResponse(JetGetJobIdsByNameCodec.encodeRequest(name),
                response -> JetGetJobIdsByNameCodec.decodeResponse(response).response);
    }

    @Override
    public Job newJobProxy(long jobId, DAG dag, JobConfig config) {
        return new ClientJobProxy(this, jobId, dag, config);
    }

    @Override
    public Job newJobProxy(long jobId) {
        return new ClientJobProxy(this, jobId);
    }

    @Override
    public ILogger getLogger() {
        return client.getLoggingService().getLogger(getClass());
    }

    private <S> S invokeRequestOnMasterAndDecodeResponse(ClientMessage request,
                                                         Function<ClientMessage, Object> decoder) {
        UUID masterUuid = client.getClientClusterService().getMasterMember().getUuid();
        return invokeRequestAndDecodeResponse(masterUuid, request, decoder);
    }

    private <S> S invokeRequestOnAnyMemberAndDecodeResponse(ClientMessage request,
                                                            Function<ClientMessage, Object> decoder) {
        return invokeRequestAndDecodeResponse(null, request, decoder);
    }

    private <S> S invokeRequestAndDecodeResponse(UUID uuid, ClientMessage request,
                                                 Function<ClientMessage, Object> decoder) {
        ClientInvocation invocation = new ClientInvocation(client, request, null, uuid);
        try {
            ClientMessage response = invocation.invoke().get();
            return serializationService.toObject(decoder.apply(response));
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

}
