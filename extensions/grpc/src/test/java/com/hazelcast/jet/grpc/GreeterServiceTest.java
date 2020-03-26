/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.grpc;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.grpc.greeter.GreeterGrpc;
import com.hazelcast.jet.grpc.greeter.GreeterGrpc.GreeterStub;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloReply;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloRequest;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.hazelcast.jet.grpc.GrpcServices.bidirectionalStreamingService;
import static com.hazelcast.jet.grpc.GrpcServices.unaryService;

public class GreeterServiceTest extends JetTestSupport {

    private JetInstance jet;
    private int port;

    @Before
    public void setup() throws IOException {
        Server server = ServerBuilder.forPort(0).addService(new GreeterServiceImpl()).build();
        server.start();
        port = server.getPort();

        jet = createJetMember();
    }

    @Test
    public void testBidirectionalStreaming() {
        final int localPort = port;
        Pipeline p = Pipeline.create();
        ServiceFactory<?, BidirectionalStreamingService<HelloRequest, HelloReply>> greeterService =
                bidirectionalStreamingService(
                        () -> ManagedChannelBuilder.forAddress("localhost", localPort).usePlaintext(),
                        channel -> {
                            GreeterStub stub = GreeterGrpc.newStub(channel);
                            return stub::sayHelloBidirectional;
                        }
                );

        p.readFrom(TestSources.items("one", "two", "three", "four"))
         .map(item -> HelloRequest.newBuilder().setName(item).build())
         .mapUsingServiceAsync(greeterService, BidirectionalStreamingService::call)
         .writeTo(Sinks.logger());

        jet.newJob(p).join();
    }

    @Test
    public void testUnary() {
        final int localPort = port;
        Pipeline p = Pipeline.create();
        ServiceFactory<?, UnaryService<HelloRequest, HelloReply>> greeterService =
                unaryService(
                        () -> ManagedChannelBuilder.forAddress("localhost", localPort).usePlaintext(),
                        channel -> {
                            GreeterStub stub = GreeterGrpc.newStub(channel);
                            return stub::sayHelloUnary;
                        }
                );

        p.readFrom(TestSources.items("one", "two", "three", "four"))
         .mapUsingServiceAsync(greeterService, (service, input) -> {
             HelloRequest request = HelloRequest.newBuilder().setName(input).build();
             return service.call(request).thenApply(r -> r.toString());
         })
         .writeTo(Sinks.logger());

        jet.newJob(p).join();
    }

    public static class GreeterServiceImpl extends GreeterGrpc.GreeterImplBase {

        @Override
        public void sayHelloUnary(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            HelloReply reply = HelloReply.newBuilder()
                                         .setMessage("Hello " + request.getName())
                                         .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<HelloRequest> sayHelloBidirectional(StreamObserver<HelloReply> responseObserver) {
            return new StreamObserver<HelloRequest>() {

                @Override
                public void onNext(HelloRequest value) {
                    HelloReply reply = HelloReply.newBuilder()
                                                 .setMessage("Hello " + value.getName())
                                                 .build();

                    responseObserver.onNext(reply);
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }
    }
}
