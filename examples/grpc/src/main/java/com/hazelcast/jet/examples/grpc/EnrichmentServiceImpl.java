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

package com.hazelcast.jet.examples.grpc;

import com.hazelcast.jet.examples.grpc.datamodel.Broker;
import com.hazelcast.jet.examples.grpc.datamodel.Product;
import io.grpc.stub.StreamObserver;

import java.util.Map;

/**
 * Server-side implementation of the gRPC service. See {@link
 * Enrichment#enrichUsingGRPC()}.
 */
public class EnrichmentServiceImpl extends EnrichmentServiceGrpc.EnrichmentServiceImplBase {

    private final Map<Integer, Product> products;
    private final Map<Integer, Broker> brokers;

    public EnrichmentServiceImpl(Map<Integer, Product> products, Map<Integer, Broker> brokers) {
        this.products = products;
        this.brokers = brokers;
    }

    @Override
    public void productInfo(ProductInfoRequest request, StreamObserver<ProductInfoReply> responseObserver) {
        String productName = products.get(request.getId()).name();
        ProductInfoReply reply = ProductInfoReply.newBuilder().setProductName(productName).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void brokerInfo(BrokerInfoRequest request, StreamObserver<BrokerInfoReply> responseObserver) {
        String brokerName = brokers.get(request.getId()).name();
        BrokerInfoReply reply = BrokerInfoReply.newBuilder().setBrokerName(brokerName).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
