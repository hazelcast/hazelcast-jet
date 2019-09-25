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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.collection.IList;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.JobProxy;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeFileP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readListP;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class WriteFilePTest extends JetTestSupport {

    private static final JetTestInstanceFactory factory = new JetTestInstanceFactory();
    private static JetInstance instance;

    private static final Semaphore semaphore = new Semaphore(0);
    private static final AtomicLong clock = new AtomicLong();

    private Path directory;
    private Path onlyFile;
    private IList<String> list;

    @BeforeClass
    public static void beforeClass() {
        instance = factory.newMember();
        semaphore.drainPermits();
    }

    @AfterClass
    public static void afterClass() {
        factory.terminateAll();
    }

    @Before
    public void setup() throws IOException {
        directory = Files.createTempDirectory("write-file-p");
        onlyFile = directory.resolve("0");
        list = instance.getList("sourceList-" + randomString());
    }

    @After
    public void after() {
        cleanUpCluster(instance);
        IOUtil.delete(directory.toFile());
    }

    @Test
    public void when_localParallelismMoreThan1_then_multipleFiles() throws Exception {
        // Given
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<String>list(list.getName()))
         .writeTo(Sinks.files(directory.toString()))
         .setLocalParallelism(2);
        addItemsToList(0, 10);

        // When
        instance.newJob(p).join();

        // Then
        try (Stream<Path> stream = Files.list(directory)) {
            assertEquals(2, stream.count());
        }
    }

    @Test
    public void smokeTest_smallFile() throws Exception {
        // Given
        Pipeline p = buildPipeline(null, false);
        addItemsToList(0, 10);

        // When
        instance.newJob(p).join();

        // Then
        checkFileContents(0, 10, false);
    }

    @Test
    public void smokeTest_bigFile() throws Exception {
        // Given
        Pipeline p = buildPipeline(null, false);
        addItemsToList(0, 100_000);

        // When
        instance.newJob(p).join();

        // Then
        checkFileContents(0, 100_000, false);
    }

    @Test
    public void when_append_then_previousContentsOfFileIsKept() throws Exception {
        // Given
        Pipeline p = buildPipeline(null, true);
        addItemsToList(1, 10);
        try (BufferedWriter writer = Files.newBufferedWriter(onlyFile)) {
            writer.write("0");
            writer.newLine();
        }

        // When
        instance.newJob(p).join();

        // Then
        checkFileContents(0, 10, false);
    }

    @Test
    public void when_overwrite_then_previousContentsOverwritten() throws Exception {
        // Given
        Pipeline p = buildPipeline(null, false);
        addItemsToList(0, 10);
        try (BufferedWriter writer = Files.newBufferedWriter(onlyFile)) {
            writer.write("bla bla");
            writer.newLine();
        }

        // When
        instance.newJob(p).join();

        // Then
        checkFileContents(0, 10, false);
    }

    @Test
    public void when_slowSource_then_fileFlushedAfterEachItem() {
        // Given
        int numItems = 10;

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", () -> new SlowSourceP(semaphore, numItems))
                           .localParallelism(1);
        Vertex sink = dag.newVertex("sink",
                writeFileP(directory.toString(), Object::toString, StandardCharsets.UTF_8, false, null, null))
                         .localParallelism(1);
        dag.edge(between(source, sink));

        Job job = instance.newJob(dag);
        for (int i = 0; i < numItems; i++) {
            // When
            semaphore.release();
            int finalI = i;
            // Then
            assertTrueEventually(() -> checkFileContents(0, finalI + 1, false), 5);
        }

        // wait for the job to finish
        job.join();
    }

    @Test
    public void testCharset() throws IOException {
        // Given
        Charset charset = Charset.forName("iso-8859-2");
        Pipeline p = buildPipeline(charset, true);
        String text = "ľščťž";
        list.add(text);

        // When
        instance.newJob(p).join();

        // Then
        assertEquals(text + System.getProperty("line.separator"), new String(Files.readAllBytes(onlyFile), charset));
    }

    @Test
    public void test_createDirectories() {
        // Given
        Path myFile = directory.resolve("subdir1/subdir2/" + onlyFile.getFileName());

        DAG dag = new DAG();
        Vertex reader = dag.newVertex("reader", readListP(list.getName()))
                           .localParallelism(1);
        Vertex writer = dag.newVertex("writer",
                writeFileP(myFile.toString(), Object::toString, StandardCharsets.UTF_8, false, null, null))
                           .localParallelism(1);
        dag.edge(between(reader, writer));
        addItemsToList(0, 10);

        // When
        instance.newJob(dag).join();

        // Then
        assertTrue(Files.exists(directory.resolve("subdir1")));
        assertTrue(Files.exists(directory.resolve("subdir1/subdir2")));
    }

    @Test
    public void when_toStringF_then_used() throws Exception {
        // Given
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<String>list(list.getName()))
         .writeTo(Sinks.<String>filesBuilder(directory.toString())
                       .toStringFn(val -> Integer.toString(Integer.parseInt(val) - 1))
                       .build());

        addItemsToList(1, 11);

        // When
        instance.newJob(p).join();

        // Then
        checkFileContents(0, 10, false);
    }

    @Test
    public void test_rollByDate() {
        int numItems = 10;
        DAG dag = new DAG();
        Vertex src = dag.newVertex("src", () -> new SlowSourceP(semaphore, numItems)).localParallelism(1);
        @SuppressWarnings("Convert2MethodRef")
        Vertex sink = dag.newVertex("sink", WriteFileP.metaSupplier(
                directory.toString(), Objects::toString, "utf-8", true, "SSS", null,
                (LongSupplier & Serializable) () -> clock.get()));
        dag.edge(between(src, sink));

        Job job = instance.newJob(dag);

        for (int i = 0; i < numItems; i++) {
            // When
            semaphore.release();
            String stringValue = i + System.lineSeparator();
            // Then
            Path file = directory.resolve(String.format("%03d-0", i));
            assertTrueEventually(() -> assertTrue("file not found: " + file, Files.exists(file)), 5);
            assertTrueEventually(() ->
                    assertEquals(stringValue, new String(Files.readAllBytes(file), StandardCharsets.UTF_8)), 5);
            clock.incrementAndGet();
        }

        job.join();
    }

    @Test
    public void test_rollByFileSize() throws IOException {
        int numItems = 10;
        DAG dag = new DAG();
        Vertex src = dag.newVertex("src", () -> new SlowSourceP(semaphore, numItems)).localParallelism(1);
        Vertex map = dag.newVertex("map", mapP((Integer i) -> i + 100));
        // maxFileSize is always large enough for 1 item but never for 2, both with windows and linux newlines
        long maxFileSize = 6L;
        Vertex sink = dag.newVertex("sink", WriteFileP.metaSupplier(
                directory.toString(), Objects::toString, "utf-8", true, null, maxFileSize));
        dag.edge(between(src, map));
        dag.edge(between(map, sink));

        Job job = instance.newJob(dag);

        // Then
        for (int i = 0; i < numItems; i++) {
            semaphore.release();
            int finalI = i;
            assertTrueEventually(() -> checkFileContents(100, finalI + 101, false));
        }
        for (int i = 0, j = 100; i < numItems / 2; i++) {
            Path file = directory.resolve("0-" + i);
            assertEquals((j++) + System.lineSeparator() + (j++) + System.lineSeparator(),
                    new String(Files.readAllBytes(file)));
        }

        job.join();
    }

    @Test
    public void test_transactional_noSnapshot() throws IOException {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<String>list(list.getName()))
         .writeTo(Sinks.<String>filesBuilder(directory.toString())
                 .build());

        addItemsToList(0, 10);

        // When
        JobConfig config = new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(HOURS.toMillis(1));
        instance.newJob(p, config).join();

        // Then
        checkFileContents(0, 10, false);
    }

    @Test
    public void test_transactional_snapshots_noRestarts() throws IOException {
        DAG dag = new DAG();
        int numItems = 5;
        Vertex source = dag.newVertex("source", () -> new SlowSourceP(semaphore, numItems))
                           .localParallelism(1);
        Vertex sink = dag.newVertex("sink",
                writeFileP(directory.toString(), Object::toString, StandardCharsets.UTF_8, false, null, null))
                         .localParallelism(1);
        dag.edge(between(source, sink));

        JobConfig config = new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(500);
        Job job = instance.newJob(dag, config);
        assertJobStatusEventually(job, RUNNING);

        JobRepository jr = new JobRepository(instance);
        waitForFirstSnapshot(jr, job.getId(), 10, true);
        for (int i = 0; i < numItems; i++) {
            waitForNextSnapshot(jr, job.getId(), 10, true);
            semaphore.release();
        }
        job.join();

        checkFileContents(0, numItems, false);
    }

    @Test
    public void test_transactional_withGracefulRestarts() throws IOException {
        test_transactional_withRestarts(true);
    }

    @Test
    public void test_transactional_withForcefulRestarts() throws IOException {
        test_transactional_withRestarts(false);
    }

    private void test_transactional_withRestarts(boolean graceful) throws IOException {
        int numItems = 500;
        Pipeline p = Pipeline.create();
        p.readFrom(SourceBuilder.stream("src", procCtx -> new int[1])
                                .fillBufferFn((ctx, buf) -> {
                                    if (ctx[0] < numItems) {
                                        buf.add(ctx[0]++);
                                        sleepMillis(5);
                                    }
                                })
                                .createSnapshotFn(ctx -> ctx[0])
                                .restoreSnapshotFn((ctx, state) -> ctx[0] = state.get(0))
                                .build())
         .withoutTimestamps()
         .writeTo(Sinks.filesBuilder(directory.toString())
                       .build());

        JobConfig config = new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(50);
        JobProxy job = (JobProxy) instance.newJob(p, config);

        long start = System.nanoTime();
        do {
            assertJobStatusEventually(job, RUNNING);
            sleepMillis(100);
            job.restart(graceful);
            try {
                checkFileContents(0, numItems, true);
                // if content matches, break the loop. Otherwise restart and try again
                break;
            } catch (AssertionError ignored) {
            }
        } while (NANOSECONDS.toSeconds(System.nanoTime() - start) < 60);

        waitForNextSnapshot(new JobRepository(instance), job.getId(), 10, true);
        ditchJob(job);
        // when the job is cancelled, there should be no temporary files
        checkFileContents(0, numItems, false);
    }

    private static class SlowSourceP extends AbstractProcessor {

        private final Semaphore semaphore;
        private final int limit;
        private int number;

        SlowSourceP(Semaphore semaphore, int limit) {
            this.semaphore = semaphore;
            this.limit = limit;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            while (number < limit) {
                if (!semaphore.tryAcquire()) {
                    return false;
                }
                assertTrue(tryEmit(number));
                number++;
            }
            return true;
        }

        @Override
        public boolean saveToSnapshot() {
            return tryEmitToSnapshot("key", number);
        }

        @Override
        protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
            assertEquals("key", key);
            number = (int) value;
        }
    }

    private void checkFileContents(int numFrom, int numTo, boolean ignoreTempFiles) throws IOException {
        String actual = Files.list(directory)
                .peek(f -> {
                    if (!ignoreTempFiles && f.getFileName().toString().endsWith(WriteFileP.TEMP_FILE_SUFFIX)) {
                        throw new IllegalArgumentException("Temp file found: " + f);
                    }
                })
                .filter(f -> !f.toString().endsWith(WriteFileP.TEMP_FILE_SUFFIX))
                .sorted(Comparator.comparing(f -> Integer.parseInt(f.getFileName().toString().substring(2))))
                .map(file -> uncheckCall(() -> Files.readAllBytes(file)))
                .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .collect(joining());

        StringBuilder expected = new StringBuilder();
        for (int i = numFrom; i < numTo; i++) {
            expected.append(i).append(System.getProperty("line.separator"));
        }

        assertEquals(expected.toString(), actual);
    }

    private void addItemsToList(int from, int to) {
        for (int i = from; i < to; i++) {
            list.add(String.valueOf(i));
        }
    }

    private Pipeline buildPipeline(Charset charset, boolean append) {
        if (charset == null) {
            charset = StandardCharsets.UTF_8;
        }
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<String>list(list.getName()))
         .writeTo(Sinks.<String>filesBuilder(directory.toString())
                 .toStringFn(Objects::toString)
                 .charset(charset)
                 .append(append)
                 .build());
        return p;
    }
}
