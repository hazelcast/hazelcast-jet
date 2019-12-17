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

package com.hazelcast.jet.impl.observer;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.Observer;
import com.hazelcast.jet.impl.execution.DoneItem;
import com.hazelcast.logging.ILogger;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.impl.executionservice.ExecutionService;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

public class ObservableImpl<T> implements Observable<T> {

    private final ConcurrentMap<UUID, RingbufferListener<T>> listeners = new ConcurrentHashMap<>();
    private final String name;
    private final HazelcastInstance hzInstance;
    private final Consumer<Observable<T>> onDestroy;
    private final ILogger logger;

    public ObservableImpl(String name, HazelcastInstance hzInstance, Consumer<Observable<T>> onDestroy, ILogger logger) {
        this.name = name;
        this.hzInstance = hzInstance;
        this.onDestroy = onDestroy;
        this.logger = logger;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public UUID addObserver(@Nonnull Observer<T> observer) {
        UUID id = UuidUtil.newUnsecureUUID();
        RingbufferListener<T> listener = new RingbufferListener<>(name, observer, hzInstance, logger);
        listeners.put(id, listener);
        listener.next();
        return id;
    }

    @Override
    public void removeObserver(UUID registrationId) {
        RingbufferListener<T> listener = listeners.remove(registrationId);
        if (listener == null) {
            throw new IllegalArgumentException(
                    String.format("No registered observer with registration ID %s", registrationId));
        } else {
            listener.cancel();
        }
    }

    @Override
    public void destroy() {
        listeners.keySet().forEach(this::removeObserver);
        listeners.clear();

        onDestroy.accept(this);
    }

    private static class RingbufferListener<T> {

        private static final int BATCH_SIZE = 64;

        private final Observer<T> observer;

        private final Ringbuffer<Object> ringbuffer;
        private final ILogger logger;
        private final Executor executor;

        private long sequence;
        private volatile boolean cancelled;

        RingbufferListener(
                String observableName,
                Observer<T> observer,
                HazelcastInstance hzInstance,
                ILogger logger
        ) {
            this.observer = observer;
            this.ringbuffer = hzInstance.getRingbuffer(ObservableUtil.getRingbufferName(observableName));
            this.logger = logger;
            this.executor = getExecutor(hzInstance);

            this.sequence = ringbuffer.headSequence();
        }

        void next() {
            if (cancelled) {
                return;
            }
            ringbuffer.readManyAsync(sequence, 1, BATCH_SIZE, null)
                    .whenCompleteAsync(this::accept, executor);
        }

        void cancel() {
            cancelled = true;
        }

        private void accept(ReadResultSet<Object> resultSet, Throwable throwable) {
            if (cancelled) {
                return;
            }
            if (throwable == null) {
                // we process all messages in batch. So we don't release the thread and reschedule ourselves;
                // but we'll process whatever was received in 1 go.
                for (Object result : resultSet) {
                    try {
                        onNewMessage(result);
                    } catch (Throwable t) {
                        logger.warning("Terminating message listener on ring-buffer " + ringbuffer.getName() +
                                ". Reason: Unhandled exception, message: " + t.getMessage(), t);
                        cancel();
                        return;
                    }

                    sequence++;
                }
                next();
            } else {
                if (handleInternalException(throwable)) {
                    next();
                } else {
                    cancel();
                }
            }
        }

        private void onNewMessage(Object message) {
            if (message instanceof Throwable) {
                observer.onError((Throwable) message);
            } else if (message instanceof DoneItem) {
                observer.onComplete();
            } else {
                //noinspection unchecked
                observer.onNext((T) message);
            }
        }

        /**
         * @param t throwable to check if it is terminal or can be handled so that listening can continue
         * @return true if the exception was handled and the listener may continue reading
         */
        protected boolean handleInternalException(Throwable t) {
            if (t instanceof OperationTimeoutException) {
                return handleOperationTimeoutException();
            } else if (t instanceof IllegalArgumentException) {
                return handleIllegalArgumentException((IllegalArgumentException) t);
            } else if (t instanceof StaleSequenceException) {
                return handleStaleSequenceException((StaleSequenceException) t);
            } else if (t instanceof HazelcastInstanceNotActiveException) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Terminating message listener on ring-buffer " + ringbuffer.getName() + ". "
                            + " Reason: HazelcastInstance is shutting down");
                }
            } else if (t instanceof DistributedObjectDestroyedException) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Terminating message listener on ring-buffer " + ringbuffer.getName() + ". "
                            + "Reason: Topic is destroyed");
                }
            } else {
                logger.warning("Terminating message listener on ring-buffer " + ringbuffer.getName() + ". "
                        + "Reason: Unhandled exception, message: " + t.getMessage(), t);
            }
            return false;
        }

        private boolean handleOperationTimeoutException() {
            if (logger.isFinestEnabled()) {
                logger.finest("Message listener on ring-buffer " + ringbuffer.getName() + " timed out. "
                        + "Continuing from last known sequence: " + sequence);
            }
            return true;
        }

        /**
         * Handles the {@link IllegalArgumentException} associated with requesting
         * a sequence larger than the {@code tailSequence + 1}.
         * This may indicate that an entire partition or an entire ring-buffer was
         * lost.
         *
         * @param t the exception
         * @return if the exception was handled and the listener may continue reading
         */
        private boolean handleIllegalArgumentException(IllegalArgumentException t) {
            final long currentHeadSequence = ringbuffer.headSequence();
            if (logger.isFinestEnabled()) {
                logger.finest(String.format("Message listener on ring-buffer %s requested a too " +
                                "large sequence: %s. Jumping from old sequence %s to sequence %s.",
                        ringbuffer.getName(), t.getMessage(), sequence, currentHeadSequence));
            }
            this.sequence = currentHeadSequence;
            return true;
        }

        /**
         * Handles a {@link StaleSequenceException} associated with requesting
         * a sequence older than the {@code headSequence}.
         * This may indicate that the reader was too slow and items in the
         * ring-buffer were already overwritten.
         *
         * @param staleSequenceException the exception
         * @return if the exception was handled and the listener may continue reading
         */
        private boolean handleStaleSequenceException(StaleSequenceException staleSequenceException) {
            long headSeq = staleSequenceException.getHeadSeq();
            if (logger.isFinestEnabled()) {
                logger.finest("Message listener on ring-buffer " + ringbuffer.getName() + " ran " +
                        "into a stale sequence. Jumping from oldSequence " + sequence + " to sequence " + headSeq + ".");
            }
            sequence = headSeq;
            return true;
        }

        private static Executor getExecutor(HazelcastInstance hzInstance) {
            if (hzInstance instanceof HazelcastInstanceImpl) {
                return ((HazelcastInstanceImpl) hzInstance).node.getNodeEngine().getExecutionService()
                        .getExecutor(ExecutionService.ASYNC_EXECUTOR);
            } else if (hzInstance instanceof HazelcastClientInstanceImpl) {
                return ((HazelcastClientInstanceImpl) hzInstance).getClientExecutionService().getUserExecutor();
            } else {
                throw new RuntimeException(String.format("Unhandled %s type: %s", HazelcastInstance.class.getSimpleName(),
                        hzInstance.getClass().getName()));
            }
        }
    }

}
