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

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.row.Row;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.TIMEOUT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JetQueryResultProducerTest extends JetTestSupport {

    @Test
    public void smokeTest() throws Exception {
        JetQueryResultProducer p = new JetQueryResultProducer();
        Semaphore semaphore = new Semaphore(0);
        Future<?> future = spawn(() -> {
            try {
                ResultIterator<Row> iterator = p.iterator();
                assertEquals(TIMEOUT, iterator.hasNext(0, TimeUnit.SECONDS));
                semaphore.release();
                assertTrue(iterator.hasNext());
                assertInstanceOf(Row.class, iterator.next());
                semaphore.release();
                assertFalse(iterator.hasNext());
                assertThatThrownBy(iterator::next)
                        .isInstanceOf(NoSuchElementException.class);
                semaphore.release();
            } catch (Throwable t) {
                logger.info("", t);
                throw t;
            }
        });

        semaphore.acquire();
        // now we're after the hasNextImmediately call in the thread

        // check that the thread is blocked in `hasNext` - that it did not release the 2nd permit
        sleepMillis(50);
        assertEquals(0, semaphore.availablePermits());

        ArrayDequeInbox inbox = new ArrayDequeInbox(new ProgressTracker());
        inbox.queue().add(new Object[0]);
        p.consume(inbox);

        // 2nd permit - the row returned from the iterator
        semaphore.acquire();

        // check that the thread is blocked in `hasNext` - that it did not release the 2nd permit
        sleepMillis(50);
        assertEquals(0, semaphore.availablePermits());

        p.done();

        assertTrueEventually(future::isDone, 5);
        semaphore.acquire();

        // called for the side-effect of throwing the exception if it happened in the thread
        future.get();
    }

    @Test
    public void when_done_then_remainingItemsIterated() {
        JetQueryResultProducer p = new JetQueryResultProducer();
        ArrayDequeInbox inbox = new ArrayDequeInbox(new ProgressTracker());
        inbox.queue().add(new Object[] {1});
        inbox.queue().add(new Object[] {2});
        p.consume(inbox);
        p.done();

        ResultIterator<Row> iterator = p.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(1, (int) iterator.next().get(0));
        assertTrue(iterator.hasNext());
        assertEquals(2, (int) iterator.next().get(0));
        assertFalse(iterator.hasNext());
    }
}
