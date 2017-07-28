/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.impl.execution.BlockingProcessorTasklet.JobFutureCompleted;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.JetTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BlockingSnapshotStorageImplTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final Queue<Object> queue = new OneToOneConcurrentArrayQueue<>(1);
    private final InternalSerializationService serializer = new DefaultSerializationServiceBuilder().build();
    private final BlockingSnapshotStorageImpl ss = new BlockingSnapshotStorageImpl(serializer, queue);
    private final CompletableFuture<Void> jobFuture = new CompletableFuture<>();
    private final Semaphore doneSemaphore = new Semaphore(0);
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private Thread thread;

    @Before
    public void before() {
        ss.initJobFuture(jobFuture);
        thread = startThread(doneSemaphore, error, ss);
    }

    private void common_when_queueFull_then_offerBlocks() throws InterruptedException {
        // wait for the thread to insert first item
        assertTrueEventually(() -> assertEquals(1, queue.size()), 1);
        assertEquals(Util.entry(serializer.toData("k1"), serializer.toData("v")), queue.peek());

        // sleep for a while and make the thread to block on offer()
        Thread.sleep(500);
        assertEquals(1, queue.size());
        // check that the thread still didn't get through the second offer()
        assertFalse(doneSemaphore.tryAcquire());
    }

    @Test
    public void when_queueEmptied_then_offerSucceeds() throws Throwable {
        common_when_queueFull_then_offerBlocks();

        // now the thread will be allowed to offer the second element and finish
        queue.clear();

        assertTrueEventually(() -> assertEquals(1, queue.size()), 1);
        assertEquals(Util.entry(serializer.toData("k2"), serializer.toData("v")), queue.peek());

        thread.join();

        if (error.get() != null) {
            throw error.get();
        }
    }

    @Test
    public void when_jobFutureCancelled_then_offerInterrupted() throws Throwable {
        common_when_queueFull_then_offerBlocks();

        // now cancel the jobFuture
        jobFuture.cancel(true);
        thread.join();

        assertEquals(1, queue.size());
        assertEquals(Util.entry(serializer.toData("k1"), serializer.toData("v")), queue.peek());

        assertInstanceOf(JobFutureCompleted.class, error.get());
        assertFalse(doneSemaphore.tryAcquire());
    }

    private Thread startThread(Semaphore doneSemaphore, AtomicReference<Throwable> error, SnapshotStorageImpl ss) {
        Thread thread = new Thread(() -> {
            try {
                assertTrue(ss.offer("k1", "v"));
                // this call should block
                assertTrue(ss.offer("k2", "v"));
                doneSemaphore.release();
            } catch (Throwable t) {
                t.printStackTrace();
                error.set(t);
            }
        });
        thread.start();
        return thread;
    }
}
