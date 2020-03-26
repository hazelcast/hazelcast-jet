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

package com.hazelcast.jet.examples.grpc;

import com.hazelcast.jet.examples.grpc.BrokerServiceGrpc.BrokerServiceImplBase;
import com.hazelcast.jet.examples.grpc.datamodel.Broker;
import io.grpc.stub.StreamObserver;

import java.util.Map;

/**
 * Server-side implementation of a gRPC service. See {@link
 * GRPCEnrichment#enrichUsingGRPC()} ()}.
 */
public class BrokerServiceImpl extends BrokerServiceImplBase {

    private final Map<Integer, Broker> brokers;

    public BrokerServiceImpl(Map<Integer, Broker> brokers) {
        this.brokers = brokers;
    }

    @Override
    public void brokerInfo(BrokerInfoRequest request, StreamObserver<BrokerInfoReply> responseObserver) {
        String brokerName = brokers.get(request.getId()).name();
        BrokerInfoReply reply = BrokerInfoReply.newBuilder().setBrokerName(brokerName).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
