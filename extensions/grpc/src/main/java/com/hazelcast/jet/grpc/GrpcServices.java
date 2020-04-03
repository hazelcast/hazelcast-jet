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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.grpc.impl.BidirectionalStreamingService;
import com.hazelcast.jet.grpc.impl.UnaryService;
import com.hazelcast.jet.pipeline.ServiceFactory;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nonnull;

/**
 * TODO
 *
 * @since 4.1
 */
public final class GrpcServices {

    private GrpcServices() {
    }

    /**
     * TODO
     *
     * @since 4.1
     */
    @Nonnull
    public static <I, O> ServiceFactory<?, GrpcService<I, O>> unaryService(
            @Nonnull SupplierEx<ManagedChannelBuilder<?>> channelFn,
            @Nonnull FunctionEx<ManagedChannel, BiConsumerEx<I, StreamObserver<O>>> createStubFn
    ) {
        return ServiceFactory.withCreateContextFn(ctx -> channelFn.get().build())
                .<GrpcService<I, O>>withCreateServiceFn((ctx, channel) ->
                        new UnaryService<>(channel, createStubFn)
                ).withDestroyServiceFn(s -> ((UnaryService<I, O>) s).destroy())
                 .withDestroyContextFn(ManagedChannel::shutdown);
    }

    /**
     * TODO
     *
     * @since 4.1
     */
    @Nonnull
    public static <I, O> ServiceFactory<?, GrpcService<I, O>> bidirectionalStreamingService(
            @Nonnull SupplierEx<ManagedChannelBuilder<?>> channelFn,
            @Nonnull FunctionEx<ManagedChannel, FunctionEx<StreamObserver<O>, StreamObserver<I>>> createStubFn
    ) {
        return ServiceFactory.withCreateContextFn(ctx -> channelFn.get().build())
                .<GrpcService<I, O>>withCreateServiceFn((ctx, channel) ->
                        new BidirectionalStreamingService<>(ctx, channel, createStubFn)
                ).withDestroyServiceFn(s -> ((BidirectionalStreamingService<I, O>) s).destroy())
                 .withDestroyContextFn(ManagedChannel::shutdown);
    }
}
