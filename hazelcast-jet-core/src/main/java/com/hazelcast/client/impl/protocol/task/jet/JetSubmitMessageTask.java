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

package com.hazelcast.client.impl.protocol.task.jet;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetSubmitCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.impl.operation.ApplicationSubmitOperation;
import com.hazelcast.jet.impl.operation.JetOperation;
import com.hazelcast.nio.Connection;

public class JetSubmitMessageTask extends JetMessageTask<JetSubmitCodec.RequestParameters> {
    public JetSubmitMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected JetSubmitCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return JetSubmitCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return JetSubmitCodec.encodeResponse(true);
    }

    @Override
    protected String getApplicationName() {
        return this.parameters.name;
    }

    @Override
    protected JetOperation prepareOperation() {
        DAG dag = this.serializationService.toObject(this.parameters.dag);
        return new ApplicationSubmitOperation(getApplicationName(), dag);
    }

    @Override
    public String getMethodName() {
        return "submit";
    }
}
