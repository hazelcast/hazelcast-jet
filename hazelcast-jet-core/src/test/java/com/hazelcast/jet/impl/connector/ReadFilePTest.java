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
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Processors.readFile;
import static com.hazelcast.jet.Processors.writeList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ReadFilePTest extends JetTestSupport{

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
    public void test_smallFiles() throws IOException, InterruptedException, ExecutionException {
        DAG dag = buildDag(null);

        File file1 = new File(directory, randomName());
        appendToFile(file1, "hello", "world");
        File file2 = new File(directory, randomName());
        appendToFile(file2, "hello2", "world2");

        Future<Void> jobFuture = instance.newJob(dag).execute();

        assertTrueEventually(() -> assertEquals(4, list.size()), 2);

        finishDirectory(jobFuture, file1, file2);
    }

    @Test
    public void test_largeFile() throws IOException, InterruptedException, ExecutionException {
        DAG dag = buildDag(null);

        File file1 = new File(directory, randomName());
        final int listLength = 10000;
        appendToFile(file1, IntStream.range(0, listLength).mapToObj(String::valueOf).toArray(String[]::new));

        Future<Void> jobFuture = instance.newJob(dag).execute();

        assertTrueEventually(() -> assertEquals(listLength, list.size()), 2);

        finishDirectory(jobFuture, file1);
    }

    @Test
    public void when_glob_the_useGlob() throws IOException, InterruptedException, ExecutionException {
        DAG dag = buildDag("file2.*");

        File file1 = new File(directory, "file1.txt");
        appendToFile(file1, "hello", "world");
        File file2 = new File(directory, "file2.txt");
        appendToFile(file2, "hello2", "world2");

        Future<Void> jobFuture = instance.newJob(dag).execute();

        assertTrueEventually(() -> assertEquals(Arrays.asList("hello2", "world2"), new ArrayList<>(list)), 2);

        // sleep little more to check, that file1 is not picked up
        sleepAtLeastMillis(500);
        assertEquals(Arrays.asList("hello2", "world2"), new ArrayList<>(list));

        finishDirectory(jobFuture, file1, file2);
    }

    private DAG buildDag(String glob) {
        DAG dag = new DAG();
        Vertex reader = dag.newVertex("reader", readFile(directory.getPath(), StandardCharsets.UTF_8, glob))
                .localParallelism(1);
        Vertex writer = dag.newVertex("writer", writeList(list.getName())).localParallelism(1);
        dag.edge(between(reader, writer));
        return dag;
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

    private void finishDirectory(Future<Void> jobFuture, File ... files) throws InterruptedException, ExecutionException {
        for (File file : files) {
            assertTrue(file.delete());
        }
        assertTrue(directory.delete());
        assertTrueEventually(() -> assertTrue("job should complete eventually", jobFuture.isDone()), 5);
        // called for side-effect of throwing exception, if the job failed
        jobFuture.get();
    }
}
