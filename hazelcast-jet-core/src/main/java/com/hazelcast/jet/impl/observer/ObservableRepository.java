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
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.execution.DoneItem;
import com.hazelcast.logging.ILogger;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.impl.executionservice.ExecutionService;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;

public final class ObservableRepository {

    /**
     * Constant ID to be used as a {@link ProcessorMetaSupplier#getTags()
     * PMS tag key} for specifying when a PMS owns an {@link Observable} (ie.
     * is the entity populating the {@link Observable} with data).
     */
    public static final String OWNED_OBSERVABLE = "owned_observable";

    /**
     * Prefix of all topic names used to back {@link Observable} implementations,
     * necessary so that such topics can't clash with regular topics used
     * for other purposes.
     */
    private static final String JET_OBSERVABLE_NAME_PREFIX = INTERNAL_JET_OBJECTS_PREFIX + "observable.";

    private ObservableRepository() {
    }

    public static void initObservable(
            String observable,
            Consumer<Object> onNewMessage,
            Consumer<Long> onSequenceNo,
            JetInstance jet,
            ILogger logger
    ) {
        Ringbuffer<Object> ringbuffer = getRingBuffer(jet.getHazelcastInstance(), observable);
        Executor executor = getExecutor(jet);
        new RingbufferListener(ringbuffer, executor, onNewMessage, onSequenceNo, logger)
                .next();
    }

    public static void destroyObservable(String observable, HazelcastInstance hzInstance) {
        Ringbuffer<Object> ringbuffer = getRingBuffer(hzInstance, observable);
        ringbuffer.destroy();
    }

    public static void completeObservables(Collection<String> observables, Throwable error, HazelcastInstance hzInstance) {
        for (String observable : observables) {
            Ringbuffer<Object> ringbuffer = getRingBuffer(hzInstance, observable);
            Object completion = error == null ? DoneItem.DONE_ITEM : error;
            ringbuffer.addAsync(completion, OverflowPolicy.OVERWRITE);
        }
    }

    @Nonnull
    private static Ringbuffer<Object> getRingBuffer(HazelcastInstance instance, String observableName) {
        return instance.getRingbuffer(getRingBufferName(observableName));
    }

    @Nonnull
    public static String getRingBufferName(String observableName) {
        return JET_OBSERVABLE_NAME_PREFIX + observableName;
    }

    private static Executor getExecutor(JetInstance jet) {
        HazelcastInstance hzInstance = jet.getHazelcastInstance();
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

    private static class RingbufferListener {

        private static final int BATCH_SIZE = 64;

        private final Ringbuffer<Object> ringbuffer;
        private final Consumer<Object> onNewMessage;
        private final Consumer<Long> onSequenceNo;
        private final ILogger logger;
        private final Executor executor;

        private long sequence;
        private volatile boolean cancelled;

        RingbufferListener(
                Ringbuffer<Object> ringbuffer,
                Executor executor,
                Consumer<Object> onNewMessage,
                Consumer<Long> onSequenceNo,
                ILogger logger
        ) {
            this.ringbuffer = ringbuffer;
            this.onNewMessage = onNewMessage;
            this.onSequenceNo = onSequenceNo;
            this.logger = logger;
            this.executor = executor;

            this.sequence = ringbuffer.tailSequence() + 1;
        }

        void next() {
            if (cancelled) {
                return;
            }
            ringbuffer.readManyAsync(sequence, 1, BATCH_SIZE, null)
                    .whenCompleteAsync(this::accept, executor);
        }

        private void cancel() {
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
                        onSequenceNo.accept(sequence);
                        onNewMessage.accept(result);
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
    }

}
