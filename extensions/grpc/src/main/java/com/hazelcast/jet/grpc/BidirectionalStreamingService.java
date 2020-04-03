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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.logging.ILogger;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nonnull;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.jet.grpc.impl.GrpcUtil.wrapGrpcException;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * TODO
 * @param <I>
 * @param <O>
 */
public final class BidirectionalStreamingService<I, O> {

    private final StreamObserver<I> sink;
    private final Queue<CompletableFuture<O>> futureQueue = new ConcurrentLinkedQueue<>();
    private final CountDownLatch completionLatch = new CountDownLatch(1);
    private final ILogger logger;
    private volatile Throwable exceptionInOutputObserver;

    BidirectionalStreamingService(
            Context context,
            ManagedChannel channel,
            FunctionEx<ManagedChannel, FunctionEx<StreamObserver<O>, StreamObserver<I>>> createStubFn
    ) {
        logger = context.logger();
        sink = createStubFn.apply(channel).apply(new OutputMessageObserver());
    }

    /**
     * TODO
     * @param input
     * @return
     */
    @Nonnull
    public CompletableFuture<O> call(@Nonnull I input) {
        checkForServerError();
        CompletableFuture<O> future = new CompletableFuture<>();
        futureQueue.add(future);
        sink.onNext(input);
        return future;
    }

    private void checkForServerError() {
        if (completionLatch.getCount() == 0) {
            throw new JetException("Exception in gRPC service: " + exceptionInOutputObserver, exceptionInOutputObserver);
        }
    }

    void destroy() throws InterruptedException {
        sink.onCompleted();
        if (!completionLatch.await(1, SECONDS)) {
            logger.info("gRPC call has not completed on time");
        }
    }

    private class OutputMessageObserver implements StreamObserver<O> {
        @Override
        public void onNext(O outputItem) {
            try {
                futureQueue.remove().complete(outputItem);
            } catch (Throwable e) {
                exceptionInOutputObserver = e;
                completionLatch.countDown();
            }
        }

        @Override
        public void onError(Throwable e) {
            try {
                e = wrapGrpcException(e);

                exceptionInOutputObserver = e;
                for (CompletableFuture<O> future; (future = futureQueue.poll()) != null; ) {
                    future.completeExceptionally(e);
                }
            } finally {
                completionLatch.countDown();
            }
        }

        @Override
        public void onCompleted() {
            for (CompletableFuture<O> future; (future = futureQueue.poll()) != null; ) {
                future.completeExceptionally(new JetException("Completion signaled before the future was completed"));
            }
            completionLatch.countDown();
        }
    }
}

