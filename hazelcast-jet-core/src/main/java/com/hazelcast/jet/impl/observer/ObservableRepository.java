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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;
import static com.hazelcast.ringbuffer.impl.RingbufferService.TOPIC_RB_PREFIX;

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

    public static Object initObservable(
            String observable,
            Consumer<ObservableBatch> onNewMessage,
            Consumer<Long> onSequenceNo,
            JetInstance jet,
            ILogger logger
    ) {
        Ringbuffer<ObservableBatch> ringbuffer = getRingBuffer(jet.getHazelcastInstance(), observable);

        RingbufferListener<ObservableBatch> ringbufferListener
                = new RingbufferListener<>(ringbuffer, onNewMessage, onSequenceNo, logger);
        ringbufferListener.next();
        return ringbufferListener;
    }

    public static void destroyObservable(String observable, HazelcastInstance hzInstance) {
        Ringbuffer<ObservableBatch> ringbuffer = getRingBuffer(hzInstance, observable);
        ringbuffer.destroy();
    }

    public static void completeObservables(Collection<String> observables, Throwable error, HazelcastInstance hzInstance) {
        for (String observable : observables) {
            Ringbuffer<ObservableBatch> ringbuffer = getRingBuffer(hzInstance, observable);
            ObservableBatch completion = error == null ? ObservableBatch.endOfData() : ObservableBatch.error(error);
            ringbuffer.addAsync(completion, OverflowPolicy.OVERWRITE);
        }
    }

    public static FunctionEx<HazelcastInstance, ConsumerEx<ArrayList<Object>>> getPublishFn(String name) {
        return hzInstance -> {
            Ringbuffer<ObservableBatch> ringbuffer = getRingBuffer(hzInstance, name);
            return buffer -> {
                ringbuffer.addAsync(ObservableBatch.items(buffer), OverflowPolicy.OVERWRITE);
                buffer.clear();
            };
        };
    }

    @Nonnull
    private static Ringbuffer<ObservableBatch> getRingBuffer(HazelcastInstance intance, String observableName) {
        String topicName = JET_OBSERVABLE_NAME_PREFIX + observableName;
        String ringBufferName = TOPIC_RB_PREFIX + topicName;
        return intance.getRingbuffer(ringBufferName);
    }


    private static class RingbufferListener<E> implements BiConsumer<ReadResultSet<E>, Throwable> {

        private static final int BATCH_SIZE = 128; //TODO (PR-1729): is it an acceptable value?

        private final Ringbuffer<E> ringbuffer;
        private final Consumer<E> onNewMessage;
        private final Consumer<Long> onSequenceNo;
        private final ILogger logger;
        private final Executor executor;

        private long sequence;
        private volatile boolean cancelled;

        RingbufferListener(
                Ringbuffer<E> ringbuffer,
                Consumer<E> onNewMessage,
                Consumer<Long> onSequenceNo,
                ILogger logger
        ) {
            this.ringbuffer = ringbuffer;
            this.onNewMessage = onNewMessage;
            this.onSequenceNo = onSequenceNo;
            this.logger = logger;

            this.sequence = ringbuffer.tailSequence() + 1;
            this.executor = Executors.newSingleThreadExecutor(); //TODO (PR-1729): what executor service to use?
        }

        void next() {
            if (cancelled) {
                return;
            }
            ringbuffer.readManyAsync(sequence, 1, BATCH_SIZE, null)
                    .whenCompleteAsync(this, executor);
        }

        private void cancel() {
            cancelled = true;
        }

        @Override
        public void accept(ReadResultSet<E> result, Throwable throwable) {
            if (throwable == null) {
                // we process all messages in batch. So we don't release the thread and reschedule ourselves;
                // but we'll process whatever was received in 1 go.
                for (E e : result) {
                    if (cancelled) {
                        return;
                    }

                    try {
                        onSequenceNo.accept(sequence);
                        onNewMessage.accept(e);
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
                if (cancelled) {
                    return;
                }

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
