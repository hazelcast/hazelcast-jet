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

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.grpc.impl.BidirectionalStreamingService;
import com.hazelcast.jet.grpc.impl.UnaryService;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.ServiceFactory;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * Provides {@link ServiceFactory} implementations for calling gRPC
 * endpoints. The {@code ServiceFactory} created are designed to be
 * used with the {@link GeneralStage#mapUsingServiceAsync(ServiceFactory, BiFunctionEx) mapUsingServiceAsync}
 * transform.
 * <p>
 * Currently two types of gRPC services are supported:
 * <oul>
 *     <li>{@link #unaryService(SupplierEx, FunctionEx) unary}</li>
 *     <li>{@link #bidirectionalStreamingService(SupplierEx, FunctionEx)} (SupplierEx, FunctionEx)
 *     bidirectionalStreaming}</li>
 * </ul>
 *
 * @since 4.1
 */
public final class GrpcServices {

    private GrpcServices() {
    }

    /**
     * Creates a {@link ServiceFactory} that calls out to a
     * <a href="https://grpc.io/docs/guides/concepts/#unary-rpc">unary gRPC service</a>.
     * <p>
     * For example, given the protobuf definition below:
     * <pre>{@code
     * service Greeter {
     *   // Sends a greeting
     *   rpc SayHello (HelloRequest) returns (HelloReply) {}
     * }
     * }</pre>
     * We can create the following service factory:
     * <pre>{@code
     * ServiceFactory<?, GrpcService<HelloRequest, HelloResponse> greeterService = unaryService(
     *     () -> ManagedChannelBuilder.forAddress("localhost", 5000).usePlaintext(),
     *     channel -> GreeterGrpc.newStub(channel)::sayHello
     * );
     * }</pre>
     * where {@code GreeterGrpc} is the auto-generated class by the protobuf compiler.
     * <p>
     * The created {@link ServiceFactory} should be used in the  * used with the
     * {@link GeneralStage#mapUsingServiceAsync(ServiceFactory, BiFunctionEx) mapUsingServiceAsync}
     * transform as follows:
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     * p.readFrom(TestSources.items("one", "two", "three", "four"))
     *     .mapUsingServiceAsync(greeterService, (service, input) -> {
     *         HelloRequest request = HelloRequest.newBuilder().setName(input).build();
     *        return service.call(request);
     * })
     *  .writeTo(Sinks.logger());
     * }</pre>
     * <p>
     * Any exception returned will cause the job to fail and should be handled by
     * calling methods on the {@link CompletableFuture} returned from the {@code call}
     * method.
     *
     * @param channelFn a function to create the channel builder. A single channel
     *                  is created per Jet member and shared between the processor
     *                  instances.
     * @param callStubFn a function, which given a channel, creates the stub
     *                   and returns a function that calls the stub given the
     *                   input item and the observer. The returned function
     *                   will be used to invoke the service once per input
     *                   item.
     * @param <I> Type of the request object
     * @param <O> Type of the response object
     */
    @Nonnull
    public static <I, O> ServiceFactory<?, GrpcService<I, O>> unaryService(
            @Nonnull SupplierEx<ManagedChannelBuilder<?>> channelFn,
            @Nonnull FunctionEx<ManagedChannel, BiConsumerEx<I, StreamObserver<O>>> callStubFn
    ) {
        return ServiceFactory.withCreateContextFn(ctx -> channelFn.get().build())
                .<GrpcService<I, O>>withCreateServiceFn((ctx, channel) ->
                        new UnaryService<>(channel, callStubFn)
                ).withDestroyServiceFn(s -> ((UnaryService<I, O>) s).destroy())
                 .withDestroyContextFn(ManagedChannel::shutdown);
    }

    /**
     * Creates a {@link ServiceFactory} that calls out to a
     * <a href="https://grpc.io/docs/guides/concepts/#bidirectional-streaming-rpc">
     *     bidrectional streaming gRPC service</a>. This provides better throughput
     * compared to the {@link #unaryService(SupplierEx, FunctionEx)} unary} service
     * because request objects are streamed, rather than invoked in a simple request-
     * response pattern.
     * <p>
     * For example, given the protobuf definition below:
     * <pre>{@code
     * service Greeter {
     *   // Sends a greeting
     *   rpc SayHello (stream HelloRequest) returns (stream HelloReply) {}
     * }
     * }</pre>
     * We can create the following service factory:
     * <pre>{@code
     * ServiceFactory<?, GrpcService<HelloRequest, HelloResponse> greeterService = bidirectionalStreamingService(
     *     () -> ManagedChannelBuilder.forAddress("localhost", 5000).usePlaintext(),
     *     channel -> GreeterGrpc.newStub(channel)::sayHello
     * );
     * }</pre>
     * where {@code GreeterGrpc} is the auto-generated class by the protobuf compiler.
     * <p>
     * The created {@link ServiceFactory} should be used in the  * used with the
     * {@link GeneralStage#mapUsingServiceAsync(ServiceFactory, BiFunctionEx) mapUsingServiceAsync}
     * transform as follows:
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     * p.readFrom(TestSources.items("one", "two", "three", "four"))
     *     .mapUsingServiceAsync(greeterService, (service, input) -> {
     *         HelloRequest request = HelloRequest.newBuilder().setName(input).build();
     *        return service.call(request);
     * })
     *  .writeTo(Sinks.logger());
     * }</pre>
     * <p>
     * Any exception returned will cause the job to fail and should be handled by
     * calling methods on the {@link CompletableFuture} returned from the {@code call}
     * method.
     *
     * @param channelFn a function to create the channel builder. A single channel
     *                  is created per Jet member and shared between the processor
     *                  instances.
     * @param callStubFn a function, which given a channel, creates the stub
     *                   and returns a function that calls the stub given the
     *                   input item and the observer. The returned function
     *                   will be used to feed input items to the input observer.
     * @param <I> Type of the request object
     * @param <O> Type of the response object
     */
    @Nonnull
    public static <I, O> ServiceFactory<?, GrpcService<I, O>> bidirectionalStreamingService(
            @Nonnull SupplierEx<ManagedChannelBuilder<?>> channelFn,
            @Nonnull FunctionEx<ManagedChannel, FunctionEx<StreamObserver<O>, StreamObserver<I>>> callStubFn
    ) {
        return ServiceFactory.withCreateContextFn(ctx -> channelFn.get().build())
                .<GrpcService<I, O>>withCreateServiceFn((ctx, channel) ->
                        new BidirectionalStreamingService<>(ctx, channel, callStubFn)
                ).withDestroyServiceFn(s -> ((BidirectionalStreamingService<I, O>) s).destroy())
                 .withDestroyContextFn(ManagedChannel::shutdown);
    }
}
