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
import com.hazelcast.client.impl.protocol.codec.JetGetJobSubmissionTimeCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.jet.impl.operation.GetJobSubmissionTimeOperation;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;

public class JetGetJobSubmissionTimeMessageTask
        extends AbstractJetMessageTask<JetGetJobSubmissionTimeCodec.RequestParameters> {

    protected JetGetJobSubmissionTimeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection, JetGetJobSubmissionTimeCodec::decodeRequest,
                o -> JetGetJobSubmissionTimeCodec.encodeResponse((long) o));
    }

    @Override
    protected Operation prepareOperation() {
        return new GetJobSubmissionTimeOperation(parameters.jobId);
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
