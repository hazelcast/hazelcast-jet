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
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Processors.readList;
import static com.hazelcast.jet.Processors.writeFile;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WriteFilePTest extends JetTestSupport {

    private JetInstance instance;
    private Path file;
    private IStreamList<String> list;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setup() throws IOException {
        instance = createJetMember();
        file = Files.createTempFile("write-file-p", "txt");
        list = instance.getList("sourceList");
    }

    @Test
    public void when_localParallelismMoreThan1_then_fail() throws ExecutionException, InterruptedException {
        // Given
        DAG dag = buildDag(null, false);
        dag.getVertex("writer").localParallelism(2);
        addItemsToList(0, 10);

        // Then
        exception.expect(Exception.class);

        // When
        instance.newJob(dag).execute().get();
    }

    @Test
    public void test_smallFile() throws Exception {
        // Given
        DAG dag = buildDag(null, false);
        addItemsToList(0, 10);

        // When
        instance.newJob(dag).execute().get();

        // Then
        checkFileContents(StandardCharsets.UTF_8, 10);
    }

    @Test
    public void test_bigFile() throws Exception {
        // Given
        DAG dag = buildDag(null, false);
        addItemsToList(0, 100_000);

        // When
        instance.newJob(dag).execute().get();

        // Then
        checkFileContents(StandardCharsets.UTF_8, 100_000);
    }

    @Test
    public void testAppend() throws Exception {
        // Given
        DAG dag = buildDag(null, true);
        addItemsToList(1, 10);
        try (Writer writer = Files.newBufferedWriter(file)) {
            writer.write("0\n");
        }

        // When
        instance.newJob(dag).execute().get();

        // Then
        checkFileContents(StandardCharsets.UTF_8, 10);
    }

    @Test
    public void testEarlyFlush() throws Exception {
        // Given
        Semaphore semaphore = new Semaphore(0);
        int numItems = 10;

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", () -> new SlowSourceP(semaphore, numItems))
                .localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeFile(file.toString(), null, false, true))
                .localParallelism(1);
        dag.edge(between(source, sink));

        // When
        Future<Void> jobFuture = instance.newJob(dag).execute();
        for (int i = 0; i < numItems; i++) {
            semaphore.release();
            int finalI = i;
            assertTrueEventually(() -> checkFileContents(StandardCharsets.UTF_8, finalI + 1), 2);
        }

        // wait for the job to finish
        jobFuture.get();

        // Then
        checkFileContents(StandardCharsets.UTF_8, 10);
    }

    @Test
    public void testCharset() throws ExecutionException, InterruptedException, IOException {
        // Given
        Charset charset = Charset.forName("iso-8859-2");
        DAG dag = buildDag(charset, true);
        String text = "ľščťž";
        list.add(text);

        // When
        instance.newJob(dag).execute().get();

        // Then
        assertEquals(text + '\n', new String(Files.readAllBytes(file), charset));
    }

    private static class SlowSourceP implements Processor {

        private final Semaphore semaphore;
        private final int limit;
        private Outbox outbox;

        SlowSourceP(Semaphore semaphore, int limit) {
            this.semaphore = semaphore;
            this.limit = limit;
        }

        @Override
        public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
            this.outbox = outbox;
        }

        @Override
        public boolean complete() {
            int number = 0;
            while (number < limit) {
                uncheckRun(semaphore::acquire);
                outbox.add(String.valueOf(number));
                number++;
            }
            return true;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }
    }

    private void checkFileContents(Charset charset, int numTo) throws IOException {
        String actual = new String(Files.readAllBytes(file), charset);

        StringBuilder expected = new StringBuilder();
        for (int i = 0; i < numTo; i++) {
            expected.append(i).append('\n');
        }

        assertEquals(expected.toString(), actual);
    }

    private void addItemsToList(int from, int to) {
        for (int i = from; i < to; i++) {
            list.add(String.valueOf(i));
        }
    }

    private DAG buildDag(Charset charset, boolean append) {
        DAG dag = new DAG();
        Vertex reader = dag.newVertex("reader", readList(list.getName()))
                .localParallelism(1);
        Vertex writer = dag.newVertex("writer", writeFile(file.toString(), charset, append, false))
                .localParallelism(1);
        dag.edge(between(reader, writer));
        return dag;
    }

}
