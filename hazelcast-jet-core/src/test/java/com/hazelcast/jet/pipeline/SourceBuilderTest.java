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

package com.hazelcast.jet.pipeline;

import com.hazelcast.core.IList;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.ToLongFunctionEx;
import com.hazelcast.util.UuidUtil;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SourceBuilderTest extends PipelineStreamTestSupport {

    private static final String LINE_PREFIX = "line";
    private static final int PREFERRED_LOCAL_PARALLELISM = 2;

    @Test
    public void batch_fileSource() throws Exception {
        // Given
        File textFile = createTestFile();

        // When
        BatchSource<String> fileSource = SourceBuilder
                .batch("file-source", x -> fileReader(textFile))
                .<String>fillBufferFn((in, buf) -> {
                    String line = in.readLine();
                    if (line != null) {
                        buf.add(line);
                    } else {
                        buf.close();
                    }
                })
                .destroyFn(BufferedReader::close)
                .build();

        // Then
        Pipeline p = Pipeline.create();
        p.drawFrom(fileSource)
                .drainTo(sinkList());

        jet().newJob(p).join();

        assertEquals(
                IntStream.range(0, itemCount).mapToObj(i -> "line" + i).collect(toList()),
                new ArrayList<>(sinkList)
        );
    }

    @Test
    public void batch_fileSource_distributed() throws Exception {
        // Given
        File textFile = createTestFile();

        // When
        BatchSource<String> fileSource = SourceBuilder
                .batch("distributed-file-source", ctx -> fileReader(textFile))
                .<String>fillBufferFn((in, buf) -> {
                    String line = in.readLine();
                    if (line != null) {
                        buf.add(line);
                    } else {
                        buf.close();
                    }
                })
                .destroyFn(BufferedReader::close)
                .distributed(PREFERRED_LOCAL_PARALLELISM)
                .build();

        // Then
        Pipeline p = Pipeline.create();
        p.drawFrom(fileSource)
                .drainTo(sinkList());
        jet().newJob(p).join();

        Map<String, Integer> actual = sinkToBag();
        Map<String, Integer> expected = IntStream.range(0, itemCount)
                .boxed()
                .collect(Collectors.toMap(i -> "line" + i, i -> PREFERRED_LOCAL_PARALLELISM * MEMBER_COUNT));

        assertEquals(expected, actual);
    }

    @Test
    public void stream_socketSource() throws IOException {
        // Given
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            startServer(serverSocket);

            // When
            int localPort = serverSocket.getLocalPort();
            BatchSource<String> socketSource = SourceBuilder
                    .batch("socket-source", ctx -> socketReader(localPort))
                    .<String>fillBufferFn((in, buf) -> {
                        String line = in.readLine();
                        if (line != null) {
                            buf.add(line);
                        } else {
                            buf.close();
                        }
                    })
                    .destroyFn(BufferedReader::close)
                    .build();

            // Then
            Pipeline p = Pipeline.create();
            p.drawFrom(socketSource)
             .drainTo(sinkList());
            jet().newJob(p).join();
            List<String> expected = IntStream.range(0, itemCount).mapToObj(i -> "line" + i).collect(toList());
            assertEquals(expected, new ArrayList<>(sinkList));
        }
    }

    @Test
    public void stream_socketSource_distributed() throws IOException {
        // Given
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            startServer(serverSocket);

            // When
            int localPort = serverSocket.getLocalPort();
            BatchSource<String> socketSource = SourceBuilder
                    .batch("distributed-socket-source", ctx -> socketReader(localPort))
                    .<String>fillBufferFn((in, buf) -> {
                        String line = in.readLine();
                        if (line != null) {
                            buf.add(line);
                        } else {
                            buf.close();
                        }
                    })
                    .destroyFn(BufferedReader::close)
                    .distributed(PREFERRED_LOCAL_PARALLELISM)
                    .build();

            // Then
            Pipeline p = Pipeline.create();
            p.drawFrom(socketSource)
             .drainTo(sinkList());
            jet().newJob(p).join();

            Map<String, Integer> expected = IntStream.range(0, itemCount)
                    .boxed()
                    .collect(Collectors.toMap(i -> "line" + i, i -> PREFERRED_LOCAL_PARALLELISM * MEMBER_COUNT));

            assertEquals(expected, sinkToBag());
        }
    }

    @Test
    public void stream_socketSource_withTimestamps() throws IOException {
        // Given
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            startServer(serverSocket);

            // When
            int localPort = serverSocket.getLocalPort();
            ToLongFunctionEx<String> timestampFn = line -> Long.valueOf(line.substring(LINE_PREFIX.length()));

            BatchSource<String> socketSource = SourceBuilder
                    .batch("socket-source-with-timestamps", ctx -> socketReader(localPort))
                    .<String>fillBufferFn((in, buf) -> {
                        String line = in.readLine();
                        if (line != null) {
                            buf.add(line);
                        } else {
                            buf.close();
                        }
                    })
                    .destroyFn(BufferedReader::close)
                    .build();

            // Then
            Pipeline p = Pipeline.create();
            p.drawFrom(socketSource)
                    .addTimestamps(timestampFn, 0)
                    .window(tumbling(1))
                    .aggregate(AggregateOperations.counting())
                    .drainTo(sinkList());

            jet().newJob(p).join();

            List<WindowResult<Long>> expected = LongStream
                    .range(1, itemCount + 1)
                    .mapToObj(i -> new WindowResult<>(i - 1, i, 1L))
                    .collect(toList());

            assertEquals(expected, new ArrayList<>(sinkList));
        }
    }

    @Test
    public void stream_socketSource_withTimestamps_andLateness() throws IOException {
        // Given
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            startServer(serverSocket);

            // When
            int localPort = serverSocket.getLocalPort();
            FunctionEx<String, Long> timestampFn = line -> Long.valueOf(line.substring(LINE_PREFIX.length()));

            int lateness = 10;
            StreamSource<String> socketSource = SourceBuilder
                    .timestampedStream("socket-source-with-timestamps", ctx -> socketReader(localPort))
                    .<String>fillBufferFn((in, buf) -> {
                        String line = in.readLine();
                        if (line != null) {
                            buf.add(line, timestampFn.apply(line));
                        }
                    })
                    .destroyFn(BufferedReader::close)
                    .build();

            // Then
            Pipeline p = Pipeline.create();
            p.drawFrom(socketSource)
                    .withNativeTimestamps(lateness)
                    .window(tumbling(1))
                    .aggregate(AggregateOperations.counting())
                    .drainTo(sinkList());

            jet().newJob(p);

            List<WindowResult<Long>> expected = LongStream.range(1, itemCount - lateness)
                    .mapToObj(i -> new WindowResult<>(i - 1, i, 1L))
                    .collect(toList());

            assertTrueEventually(() -> assertEquals(expected, new ArrayList<>(sinkList)), 10);
        }
    }

    @Test
    public void stream_distributed_socketSource_withTimestamps() throws IOException {
        // Given
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            startServer(serverSocket);

            // When
            int localPort = serverSocket.getLocalPort();
            ToLongFunctionEx<String> timestampFn = line -> Long.valueOf(line.substring(LINE_PREFIX.length()));

            BatchSource<String> socketSource = SourceBuilder
                    .batch("socket-source-with-timestamps", ctx -> socketReader(localPort))
                    .<String>fillBufferFn((in, buf) -> {
                        String line = in.readLine();
                        if (line != null) {
                            buf.add(line);
                        } else {
                            buf.close();
                        }
                    })
                    .destroyFn(BufferedReader::close)
                    .distributed(PREFERRED_LOCAL_PARALLELISM)
                    .build();

            // Then
            Pipeline p = Pipeline.create();
            p.drawFrom(socketSource)
                    .addTimestamps(timestampFn, 1000)
                    .window(tumbling(1))
                    .aggregate(AggregateOperations.counting())
                    .drainTo(sinkList());

            jet().newJob(p).join();

            List<WindowResult<Long>> expected = LongStream.range(1, itemCount + 1)
                    .mapToObj(i -> new WindowResult<>(i - 1, i, (long) PREFERRED_LOCAL_PARALLELISM * MEMBER_COUNT))
                    .collect(toList());

            assertEquals(expected, new ArrayList<>(sinkList));
        }
    }

    @Test
    public void test_faultTolerance() {
        StreamSource<Integer> source = SourceBuilder
                .timestampedStream("src", ctx -> new NumberGeneratorContext())
                .<Integer>fillBufferFn((src, buffer) -> {
                    long expectedCount = NANOSECONDS.toMillis(System.nanoTime() - src.startTime);
                    expectedCount = Math.min(expectedCount, src.current + 100);
                    while (src.current < expectedCount) {
                        buffer.add(src.current, src.current);
                        src.current++;
                    }
                })
                .createSnapshotFn(src -> {
                    System.out.println("Will save " + src.current + " to snapshot");
                    return src;
                })
                .restoreSnapshotFn((src, states) -> {
                    assert states.size() == 1;
                    src.restore(states.get(0));
                    System.out.println("Restored " + src.current + " from snapshot");
                })
                .build();

        long windowSize = 100;
        IList<WindowResult<Long>> result = jet().getList("result-" + UuidUtil.newUnsecureUuidString());

        Pipeline p = Pipeline.create();
        p.drawFrom(source)
         .withNativeTimestamps(0)
         .window(tumbling(windowSize))
         .aggregate(AggregateOperations.counting())
         .peek()
         .drainTo(Sinks.list(result));

        Job job = jet().newJob(p, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE));
        assertTrueEventually(() -> assertFalse("result list is still empty", result.isEmpty()));
        // restart the job
        job.restart();
        assertJobStatusEventually(job, JobStatus.RUNNING);

        // wait until more results are added
        int oldSize = result.size();
        assertTrueEventually(() -> assertTrue("no more results added to the list", result.size() > oldSize));
        job.cancel();

        // results should contain a monotonic sequence of results, each with count=1000
        Iterator<WindowResult<Long>> iterator = result.iterator();
        for (int i = 0; i < result.size(); i++) {
            WindowResult<Long> next = iterator.next();
            assertEquals(windowSize, (long) next.result());
            assertEquals(i * windowSize, next.start());
        }
    }

    private static final class NumberGeneratorContext implements Serializable {
        long startTime = System.nanoTime();
        int current;

        void restore(NumberGeneratorContext other) {
            this.startTime = other.startTime;
            this.current = other.current;
        }
    }

    private void startServer(ServerSocket serverSocket) {
        spawnSafe(() -> {
            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                System.out.println("Accepted connection from " + socket.getPort());
                spawnSafe(() -> {
                    try (PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()))) {
                        for (int i = 0; i < itemCount; i++) {
                            out.println(LINE_PREFIX + i);
                        }
                    } finally {
                        socket.close();
                    }
                });
            }
        });
    }

    private File createTestFile() throws IOException {
        File dir = createTempDirectory();
        File textFile = new File(dir, "stuff.txt");
        try (PrintWriter out = new PrintWriter(new FileWriter(textFile))) {
            for (int i = 0; i < itemCount; i++) {
                out.println("line" + i);
            }
        }
        return textFile;
    }

    private static BufferedReader socketReader(int port) throws IOException {
        return new BufferedReader(new InputStreamReader(new Socket("localhost", port).getInputStream()));
    }

    private static BufferedReader fileReader(File textFile) throws FileNotFoundException {
        return new BufferedReader(new FileReader(textFile));
    }

}
