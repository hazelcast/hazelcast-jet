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

package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class JetMessageTaskFactoryProvider implements MessageTaskFactoryProvider {
    private final MessageTaskFactory[] factories = new MessageTaskFactory[Short.MAX_VALUE];
    private final Node node;

    public JetMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
        initFactories();
    }

    public void initFactories() {
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.jet
        factories[com.hazelcast.client.impl.protocol.codec.JetInterruptCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetInterruptMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.JetAcceptLocalizationCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetAcceptLocalizationMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.JetInitCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetInitMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.JetGetAccumulatorsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetGetAccumulatorsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.JetExecuteCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetExecuteMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.JetSubmitCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetSubmitMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.JetLocalizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetLocalizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.JetEventCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetEventMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task
        factories[com.hazelcast.client.impl.protocol.codec.ClientAddPartitionLostListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AddPartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientRemovePartitionLostListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.RemovePartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientCreateProxyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.CreateProxyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientGetDistributedObjectsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.GetDistributedObjectsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAddDistributedObjectListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AddDistributedObjectListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientDestroyProxyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.DestroyProxyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientPingCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.PingMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAddMembershipListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AddMembershipListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AuthenticationCustomCredentialsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientRemoveAllListenersCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.RemoveAllListenersMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientRemoveDistributedObjectListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.RemoveDistributedObjectListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientGetPartitionsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.GetPartitionsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AuthenticationMessageTask(clientMessage, node, connection);
            }
        };
//endregion​
    }

    @Override
    public MessageTaskFactory[] getFactories() {
        return factories.clone();
    }
}
