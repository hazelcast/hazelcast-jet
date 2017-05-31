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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.Processor;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.jet.processor.Sources;
import com.hazelcast.logging.ILogger;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class StreamTextSocketPTest {
    private static final String HOST = "localhost";
    private static final int PORT = 8888;

    private ArrayDequeInbox inbox;
    private ArrayDequeOutbox outbox;
    private Queue<Object> bucket;
    private ProcCtx context;

    private Processor processor;

    @Before
    public void before() {
        inbox = new ArrayDequeInbox();
        outbox = new ArrayDequeOutbox(new int[]{10}, new ProgressTracker());
        ILogger logger = mock(ILogger.class);
        context = new ProcCtx(null, logger, null, 0);
        context.initJobFuture(new CompletableFuture<>());
        bucket = outbox.queueWithOrdinal(0);

        processor = Sources.streamTextSocket(HOST, PORT).get();
        processor.init(outbox, context);
    }

    @Test
    public void smokeTest() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            new Thread(() -> uncheckRun(() -> {
                Socket socket = serverSocket.accept();
                PrintWriter writer = new PrintWriter(socket.getOutputStream());
                writer.write("hello\n");
                writer.write("world\n");
                writer.close();
                socket.close();
            })).start();

            assertTrue(processor.complete());
            assertEquals("hello", bucket.poll());
            assertEquals("world", bucket.poll());
            assertEquals(null, bucket.poll());
        }
    }

}
