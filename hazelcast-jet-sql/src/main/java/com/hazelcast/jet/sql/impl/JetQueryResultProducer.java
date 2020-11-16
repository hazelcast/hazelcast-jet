/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.internal.util.concurrent.IdleStrategy;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.Row;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.DONE;
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.TIMEOUT;
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.YES;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class JetQueryResultProducer implements QueryResultProducer {

    static final int QUEUE_CAPACITY = 4096;

    private final OneToOneConcurrentArrayQueue<Row> rows = new OneToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
    private final AtomicBoolean done = new AtomicBoolean(false);

    private InternalIterator iterator;
    private Exception exception;

    @Override
    public ResultIterator<Row> iterator() {
        if (iterator != null) {
            throw new IllegalStateException("Iterator can be requested only once");
        }
        iterator = new InternalIterator();
        return iterator;
    }

    public void consume(Inbox inbox) {
        if (done.get()) {
            throw exception == null ? new IllegalStateException("Already done") : new RuntimeException(exception);
        }
        for (Object[] row; (row = (Object[]) inbox.peek()) != null && rows.offer(new HeapRow(row)); ) {
            inbox.remove();
        }
    }

    public void done() {
        done.set(true);
    }

    @Override
    public void onError(QueryException error) {
        if (done.compareAndSet(false, true)) {
            exception = error;
        }
    }

    private class InternalIterator implements ResultIterator<Row> {

        private final IdleStrategy idler =
                new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(50), MILLISECONDS.toNanos(1));

        private Row nextRow;

        @Override
        public boolean hasNext() {
            return hasNextWait(Long.MAX_VALUE) == YES;
        }

        @Override
        public HasNextResult hasNext(long timeout, TimeUnit timeUnit) {
            if (nextRow != null || (nextRow = rows.poll()) != null) {
                return YES;
            }

            if (done.get()) {
                if ((nextRow = rows.poll()) != null) {
                    return YES;
                } else {
                    if (exception == null) {
                        return DONE;
                    } else {
                        throw new RuntimeException("The Jet SQL job failed: " + exception.getMessage(), exception);
                    }
                }
            }

            if (timeout == 0) {
                return TIMEOUT;
            }

            return hasNextWait(System.nanoTime() + timeUnit.toNanos(timeout));
        }

        private HasNextResult hasNextWait(long endTimeNanos) {
            long idleCount = 0;
            do {
                if (nextRow != null || (nextRow = rows.poll()) != null) {
                    return YES;
                }

                if (done.get()) {
                    if ((nextRow = rows.poll()) != null) {
                        return YES;
                    } else {
                        if (exception == null) {
                            return DONE;
                        } else {
                            throw new RuntimeException("The Jet SQL job failed: " + exception.getMessage(), exception);
                        }
                    }
                }

                idler.idle(++idleCount);
            } while (System.nanoTime() < endTimeNanos);
            return TIMEOUT;
        }

        @Override
        public Row next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                return nextRow;
            } finally {
                nextRow = rows.poll();
            }
        }
    }
}
