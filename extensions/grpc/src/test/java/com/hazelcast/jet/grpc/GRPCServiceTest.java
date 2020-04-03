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

import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.grpc.greeter.GreeterGrpc;
import com.hazelcast.jet.grpc.greeter.GreeterGrpc.GreeterStub;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloReply;
import com.hazelcast.jet.grpc.greeter.GreeterOuterClass.HelloRequest;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import io.grpc.BindableService;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.jet.grpc.GrpcServices.bidirectionalStreamingService;
import static com.hazelcast.jet.grpc.GrpcServices.unaryService;
import static org.junit.Assert.fail;

public class GreeterServiceTest extends TestInClusterSupport {

    Server server;

    @After
    public void teardown() {
        if (server != null) {
            server.shutdown() ;
        }
    }

    @Test
    public void when_bidirectionalStreaming() throws IOException {
        // Given
        Server server = createServer(new GreeterServiceImpl());
        final int localPort = server.getPort();

        ServiceFactory<?, BidirectionalStreamingService<HelloRequest, HelloReply>> greeterService =
                bidirectionalStreamingService(
                        () -> ManagedChannelBuilder.forAddress("localhost", localPort).usePlaintext(),
                        channel -> {
                            GreeterStub stub = GreeterGrpc.newStub(channel);
                            return stub::sayHelloBidirectional;
                        }
                );
        List<String> expected = Arrays.asList("Hello one", "Hello two", "Hello three", "Hello four");

        Pipeline p = Pipeline.create();
        BatchStage<String> stage = p.readFrom(TestSources.items("one", "two", "three", "four"));

        // When
        BatchStage<String> mapped = stage.mapUsingServiceAsync(greeterService, (service, item) -> {
            HelloRequest req = HelloRequest.newBuilder().setName(item).build();
            return service.call(req).thenApply(HelloReply::getMessage);
        });

        // Then
        mapped.writeTo(AssertionSinks.assertAnyOrder(expected));
        jet().newJob(p).join();
    }

    @Test
    public void when_unary() throws IOException {
        // Given
        Server server = createServer(new GreeterServiceImpl());
        final int localPort = server.getPort();

        Pipeline p = Pipeline.create();
        ServiceFactory<?, UnaryService<HelloRequest, HelloReply>> greeterService =
                unaryService(
                        () -> ManagedChannelBuilder.forAddress("localhost", localPort).usePlaintext(),
                        channel -> {
                            GreeterStub stub = GreeterGrpc.newStub(channel);
                            return stub::sayHelloUnary;
                        }
                );
        List<String> expected = Arrays.asList("Hello one", "Hello two", "Hello three", "Hello four");

        BatchStage<String> source = p.readFrom(TestSources.items("one", "two", "three", "four"));

        // When
        BatchStage<String> mapped = source
                .mapUsingServiceAsync(greeterService, (service, input) -> {
                    HelloRequest request = HelloRequest.newBuilder().setName(input).build();
                    return service.call(request).thenApply(HelloReply::getMessage);
                });

        // Then
        mapped.writeTo(AssertionSinks.assertAnyOrder(expected));
        jet().newJob(p).join();
    }

    @Test
    public void when_unary_with_faultyService() throws IOException {
        // Given
        Server server = createServer(new FaultyGreeterServiceImpl());
        final int localPort = server.getPort();

        Pipeline p = Pipeline.create();
        ServiceFactory<?, UnaryService<HelloRequest, HelloReply>> greeterService =
                unaryService(
                        () -> ManagedChannelBuilder.forAddress("localhost", localPort).usePlaintext(),
                        channel -> {
                            GreeterStub stub = GreeterGrpc.newStub(channel);
                            return stub::sayHelloUnary;
                        }
                );
        List<String> expected = Arrays.asList("Hello one", "Hello two", "Hello three", "Hello four");

        BatchStage<String> source = p.readFrom(TestSources.items("one", "two", "three", "four"));

        // When
        BatchStage<String> mapped = source
                .mapUsingServiceAsync(greeterService, (service, input) -> {
                    HelloRequest request = HelloRequest.newBuilder().setName(input).build();
                    return service.call(request).thenApply(HelloReply::getMessage);
                });

        // Then
        mapped.writeTo(Sinks.noop());

        try {
            jet().newJob(p).join();
            fail("Job should have failed");
        } catch (Exception e) {
            Throwable ex = ExceptionUtil.peel(e);
            ex.printStackTrace();
        }
    }

    private static Server createServer(BindableService service) throws IOException {
        Server server = ServerBuilder.forPort(0).addService(service).build();
        server.start();
        return server;
    }

    private static class FaultyGreeterServiceImpl extends GreeterGrpc.GreeterImplBase {

        @Override
        public StreamObserver<HelloRequest> sayHelloBidirectional(StreamObserver<HelloReply> responseObserver) {
            return new StreamObserver<HelloRequest>() {
                @Override
                public void onNext(HelloRequest value) {
                    responseObserver.onError(new RuntimeException("something went wrong"));
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {

                }
            };
        }

        @Override
        public void sayHelloUnary(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            responseObserver.onError(new RuntimeException("something went wrong"));
        }
    }
    private static class GreeterServiceImpl extends GreeterGrpc.GreeterImplBase {

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
