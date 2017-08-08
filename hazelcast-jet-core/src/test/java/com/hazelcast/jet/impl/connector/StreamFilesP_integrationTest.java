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

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.processor.Sources;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.processor.Sinks.writeList;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
@Ignore
public class StreamFilesP_integrationTest extends JetTestSupport {

    private JetInstance instance;
    private File directory;
    private IStreamList<String> list;

    @Before
    public void setup() throws IOException {
        instance = createJetMember();
        directory = createTempDirectory();
        list = instance.getList("writer");
    }

    @Test
    public void when_appendingToPreexisting_then_pickupNewLines() throws Exception {
        DAG dag = buildDag();

        // this is a pre-existing file, should not be picked up
        File file = createNewFile();
        appendToFile(file, "hello", "pre-existing");
        sleepAtLeastMillis(50);

        Future<Void> jobFuture = instance.newJob(dag).getFuture();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);

        // pre-existing file should not be picked up
        assertEquals(0, list.size());
        appendToFile(file, "third line");
        // now, all three lines are picked up
        assertTrueEventually(() -> assertEquals(1, list.size()));

        finishDirectory(jobFuture, file);
    }

    @Test
    public void when_appendingToPreexistingIncompleteLine_then_pickupCompleteLines() throws Exception {
        DAG dag = buildDag();

        // this is a pre-existing file, should not be picked up
        File file = createNewFile();
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(file, true))) {
            // note: no newline appended
            writer.write("hello");
        }
        sleepAtLeastMillis(50);

        Future<Void> jobFuture = instance.newJob(dag).getFuture();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);

        // pre-existing file should not be picked up
        assertEquals(0, list.size());
        // this completes the first line and a second one - only the second one should be picked
        appendToFile(file, "world", "second line");
        // now, all three lines are picked up
        assertTrueEventually(() -> assertEquals(1, list.size()));
        assertEquals("second line", list.get(0));

        finishDirectory(jobFuture, file);
    }

    @Test
    public void when_withCrlf_then_pickupCompleteLines() throws Exception {
        DAG dag = buildDag();

        // this is a pre-existing file, should not be picked up
        File file = createNewFile();
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(file, true))) {
            // note: no newline appended
            writer.write("hello world\r");
        }
        sleepAtLeastMillis(50);

        Future<Void> jobFuture = instance.newJob(dag).getFuture();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);

        // pre-existing file should not be picked up
        assertEquals(0, list.size());
        // this completes the first line and a second one - only the second one should be picked
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(file, true))) {
            // note: no newline appended
            writer.write("\nsecond line\r\n");
        }
        // now, all three lines are picked up
        assertTrueEventually(() -> assertEquals(1, list.size()));
        assertEquals("second line", list.get(0));

        finishDirectory(jobFuture, file);
    }

    @Test
    @Ignore
    public void when_newAndModified_then_pickupAddition() throws Exception {
        DAG dag = buildDag();

        Future<Void> jobFuture = instance.newJob(dag).getFuture();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);

        assertEquals(0, list.size());
        File file = new File(directory, randomName());
        appendToFile(file, "hello", "world");
        assertTrueEventually(() -> assertEquals(2, list.size()));

        // now add one more line, only this line should be picked up
        appendToFile(file, "third line");
        assertTrueEventually(() -> assertEquals(3, list.size()));

        finishDirectory(jobFuture, file);
    }

    @Test
    public void when_fileWithManyLines_then_emitCooperatively() throws Exception {
        DAG dag = buildDag();

        Future<Void> jobFuture = instance.newJob(dag).getFuture();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);

        assertEquals(0, list.size());
        File subdir = new File(directory, "subdir");
        assertTrue(subdir.mkdir());
        File fileInSubdir = new File(subdir, randomName());
        appendToFile(fileInSubdir, IntStream.range(0, 5000).mapToObj(String::valueOf).toArray(String[]::new));

        // move the file to watchedDirectory
        File file = new File(directory, fileInSubdir.getName());
        assertTrue(fileInSubdir.renameTo(file));

        assertTrueEventually(() -> assertEquals(5000, list.size()));

        finishDirectory(jobFuture, file, subdir);
    }

    @Test
    @Ignore
    public void stressTest() throws Exception {
        // Here, I will start the job and in two separate threads I'll write fixed number of lines
        // to two different log files, with few ms hiccups between each line.
        // At the end, I'll check, if all the contents matches.
        DAG dag = buildDag();

        Future<Void> jobFuture = instance.newJob(dag).getFuture();
        // wait for the processor to initialize
        sleepAtLeastSeconds(2);
        int numLines = 10_000;
        File file1 = new File(directory, "file1.log");
        File file2 = new File(directory, "file2.log");
        Thread t1 = new Thread(new RandomWriter(file1, numLines, "file0"));
        Thread t2 = new Thread(new RandomWriter(file2, numLines, "file1"));
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        // wait for the list to be full
        assertTrueEventually(() -> assertEquals(2 * numLines, list.size()));

        // check the list
        Set<Integer>[] expectedNumbers = new Set[] {
                IntStream.range(0, numLines).boxed().collect(Collectors.toSet()),
                IntStream.range(0, numLines).boxed().collect(Collectors.toSet())};

        for (String logLine : list) {
            // logLine has the form "fileN M ...", N is fileIndex, M is line number
            int fileIndex = logLine.charAt(4) - '0';
            int lineIndex = Integer.parseInt(logLine.split(" ", 3)[1]);
            assertTrue(expectedNumbers[fileIndex].remove(lineIndex));
        }

        assertEquals(Collections.emptySet(), expectedNumbers[0]);
        assertEquals(Collections.emptySet(), expectedNumbers[1]);

        finishDirectory(jobFuture, file1, file2);
    }

    private static class RandomWriter implements Runnable {

        private final File file;
        private final int numLines;
        private final String prefix;
        private final Random random = new Random();

        RandomWriter(File file, int numLines, String prefix) {
            this.file = file;
            this.numLines = numLines;
            this.prefix = prefix;
        }

        @Override
        public void run() {
            try (FileOutputStream fos = new FileOutputStream(file);
                    OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
                    BufferedWriter bw = new BufferedWriter(osw)
            ) {
                for (int i = 0; i < numLines; i++) {
                    bw.write(prefix + ' ' + i +
                            " Lorem ipsum dolor sit amet, consectetur adipiscing elit," +
                            " sed do eiusmod tempor incididunt ut labore et dolore magna aliqua\n");
                    bw.flush();
                    osw.flush();
                    fos.flush();
                    if (random.nextInt(100) < 5) {
                        LockSupport.parkNanos(MILLISECONDS.toNanos(1));
                    }
                }
            } catch (IOException e) {
                throw sneakyThrow(e);
            }
        }
    }

    private DAG buildDag() {
        DAG dag = new DAG();
        Vertex reader = dag.newVertex("reader", Sources.streamFiles(directory.getPath()))
                           .localParallelism(1);
        Vertex writer = dag.newVertex("writer", writeList(list.getName())).localParallelism(1);
        dag.edge(between(reader, writer));
        return dag;
    }

    private File createNewFile() {
        File file = new File(directory, randomName());
        assertTrueEventually(() -> assertTrue(file.createNewFile()));
        return file;
    }

    private static void appendToFile(File file, String... lines) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(file, true))) {
            for (String payload : lines) {
                writer.write(payload + '\n');
            }
        }
    }

    private static File createTempDirectory() throws IOException {
        Path directory = Files.createTempDirectory("read-file-stream-p");
        File file = directory.toFile();
        file.deleteOnExit();
        return file;
    }

    private void finishDirectory(Future<Void> jobFuture, File ... files) throws Exception {
        for (File file : files) {
            System.out.println("deleting " + file + "...");
            assertTrueEventually(() -> assertTrue("Failed to delete " + file, file.delete()));
            System.out.println("deleted " + file);
        }
        assertTrueEventually(() -> assertTrue("Failed to delete " + directory, directory.delete()));
        assertTrueEventually(() -> assertTrue("job should complete eventually", jobFuture.isDone()));
        // called for side-effect of throwing exception, if the job failed
        jobFuture.get();
    }
}
