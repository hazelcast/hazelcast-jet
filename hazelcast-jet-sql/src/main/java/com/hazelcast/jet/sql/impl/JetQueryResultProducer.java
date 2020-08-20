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
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.sql.impl.ResultIterator.HasNextImmediatelyResult.DONE;
import static com.hazelcast.sql.impl.ResultIterator.HasNextImmediatelyResult.RETRY;
import static com.hazelcast.sql.impl.ResultIterator.HasNextImmediatelyResult.YES;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class JetQueryResultProducer implements QueryResultProducer {

    private static final int QUEUE_CAPACITY = 4096;

    private final OneToOneConcurrentArrayQueue<Row> queue = new OneToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
    private final AtomicReference<QueryException> error = new AtomicReference<>();
    private volatile boolean done;

    @Override
    public ResultIterator<Row> iterator() {
        return new InternalIterator();
    }

    @Override
    public void onError(QueryException error) {
        this.error.compareAndSet(null, error);
    }

    public void done() {
        done = true;
    }

    public void consume(Inbox inbox) {
        if (error.get() != null) {
            throw new RuntimeException(error.get());
        }
        for (Object[] r; (r = (Object[]) inbox.peek()) != null && queue.offer(new HeapRow(r)); ) {
            inbox.remove();
        }
    }

    private class InternalIterator implements ResultIterator<Row> {

        private final IdleStrategy idler =
                new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(50), MILLISECONDS.toNanos(1));

        private Row nextRow;

        public InternalIterator() {
            moveNext();
        }

        private void moveNext() {
            nextRow = queue.poll();
        }

        @Override
        public HasNextImmediatelyResult hasNextImmediately() {
            return nextRow != null ? YES
                    : (done ? DONE : RETRY);
        }

        @Override
        public boolean hasNext() {
            long idleCount = 0;
            while (true) {
                if (nextRow != null) {
                    return true;
                }
                if (done) {
                    return false;
                }
                idler.idle(++idleCount);
                moveNext();
            }
        }

        @Override
        public Row next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            try {
                return nextRow;
            } finally {
                moveNext();
            }
        }
    }
}
