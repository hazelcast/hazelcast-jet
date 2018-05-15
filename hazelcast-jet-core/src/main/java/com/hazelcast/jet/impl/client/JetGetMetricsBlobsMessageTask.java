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

package com.hazelcast.jet.impl.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetGetMetricBlobsCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetMetricBlobsCodec.RequestParameters;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.nio.Connection;

import java.security.Permission;

public class JetGetMetricsBlobsMessageTask extends AbstractMessageTask<RequestParameters> {

    protected JetGetMetricsBlobsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        JetService jetService = getService(getServiceName());
        sendResponse(jetService.getJetMetricsService().getMetricBlobs(parameters.fromSequence));
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return JetGetMetricBlobsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return JetGetMetricBlobsCodec.encodeResponse(serializationService.toData(response));
    }

    @Override
    public String getServiceName() {
        return JetService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "getJobSubmissionTime";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}

