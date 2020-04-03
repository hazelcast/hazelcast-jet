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
import com.hazelcast.jet.grpc.impl.GrpcUtil;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * TODO
 * @param <I>
 * @param <O>
 */
public final class UnaryService<I, O> {

    private final BiConsumerEx<I, StreamObserver<O>> callFn;

    UnaryService(
            @Nonnull ManagedChannel channel,
            @Nonnull FunctionEx<ManagedChannel, BiConsumerEx<I, StreamObserver<O>>> createStubFn
    ) {
        callFn = createStubFn.apply(channel);
    }

    /**
     * TODO
     * @param input
     * @return
     */
    @Nonnull
    public CompletableFuture<O> call(@Nonnull I input) {
        Observer<O> o = new Observer<>();
        callFn.accept(input, o);
        return o.future;
    }

    void destroy() {
    }

    // these objects could also be pooled
    private static class Observer<O> implements StreamObserver<O> {
        private final CompletableFuture<O> future;
        private volatile O value; // TODO: is this necessary here - contract states "thread-compatible"

        Observer() {
            this.future = new CompletableFuture<>();
        }

        @Override
        public void onNext(O value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(GrpcUtil.wrapGrpcException(t));
        }

        @Override
        public void onCompleted() {
            future.complete(value);
        }
    }
}

