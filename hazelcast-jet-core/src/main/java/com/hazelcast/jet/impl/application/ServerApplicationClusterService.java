/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.application;


import com.hazelcast.core.Member;
import com.hazelcast.jet.config.ApplicationConfig;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.impl.application.localization.Chunk;
import com.hazelcast.jet.impl.operation.AcceptLocalizationOperation;
import com.hazelcast.jet.impl.operation.ApplicationEventOperation;
import com.hazelcast.jet.impl.operation.ApplicationExecuteOperation;
import com.hazelcast.jet.impl.operation.ApplicationInitOperation;
import com.hazelcast.jet.impl.operation.ApplicationInterruptOperation;
import com.hazelcast.jet.impl.operation.ApplicationSubmitOperation;
import com.hazelcast.jet.impl.operation.GetAccumulatorsOperation;
import com.hazelcast.jet.impl.operation.JetOperation;
import com.hazelcast.jet.impl.operation.LocalizationChunkOperation;
import com.hazelcast.jet.impl.statemachine.application.ApplicationEvent;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;


public class ServerApplicationClusterService extends ApplicationClusterService<JetOperation> {
    private final NodeEngine nodeEngine;

    public ServerApplicationClusterService(String name,
                                           ExecutorService executorService,
                                           NodeEngine nodeEngine) {
        super(name, executorService);

        this.nodeEngine = nodeEngine;
    }

    public JetOperation createInitApplicationInvoker(ApplicationConfig config) {
        return new ApplicationInitOperation(
                this.name,
                config
        );
    }

    @Override
    public JetOperation createInterruptInvoker() {
        return new ApplicationInterruptOperation(
                this.name
        );
    }

    @Override
    public JetOperation createExecutionInvoker() {
        return new ApplicationExecuteOperation(
                this.name
        );
    }

    @Override
    public JetOperation createAccumulatorsInvoker() {
        return new GetAccumulatorsOperation(
                this.name
        );
    }

    @Override
    public JetOperation createSubmitInvoker(DAG dag) {
        return new ApplicationSubmitOperation(
                this.name, dag);
    }

    @Override
    public JetOperation createLocalizationInvoker(Chunk chunk) {
        return new LocalizationChunkOperation(this.name, chunk);
    }

    @Override
    public JetOperation createAcceptedLocalizationInvoker() {
        return new AcceptLocalizationOperation(this.name);
    }

    public JetOperation createEventInvoker(ApplicationEvent applicationEvent) {
        return new ApplicationEventOperation(
                this.name, applicationEvent
        );
    }

    @Override
    public Set<Member> getMembers() {
        return this.nodeEngine.getClusterService().getMembers();
    }

    @Override
    protected ApplicationConfig getApplicationConfig() {
        return applicationConfig;
    }

    @Override
    protected <T> Callable<T> createInvocation(Member member,
                                            Supplier<JetOperation> factory) {
        return new ServerApplicationInvocation<>(
                factory.get(),
                member.getAddress(),
                this.nodeEngine
        );
    }

    @Override
    protected <T> T toObject(Data data) {
        return this.nodeEngine.toObject(data);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Accumulator> readAccumulatorsResponse(Callable callable) throws Exception {
        return (Map<String, Accumulator>) callable.call();
    }
}
