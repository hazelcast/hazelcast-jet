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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.test.HazelcastParallelClassRunner;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readFilesP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class ReadFilesPTest extends JetTestSupport {

    private JetInstance instance;
    private File directory;
    private IStreamList<String> list;

    @Before
    public void setup() throws Exception {
        instance = createJetMember();
        directory = createTempDirectory();
        list = instance.getList("writer");
    }

    @Test
    public void test_smallFiles() throws Exception {
        DAG dag = buildDag(null);

        File file1 = new File(directory, randomName());
        appendToFile(file1, "hello", "world");
        File file2 = new File(directory, randomName());
        appendToFile(file2, "hello2", "world2");

        instance.newJob(dag).join();

        assertEquals(4, list.size());

        finishDirectory(file1, file2);
    }

    @Test
    public void test_largeFile() throws Exception {
        DAG dag = buildDag(null);

        File file1 = new File(directory, randomName());
        final int listLength = 10000;
        appendToFile(file1, IntStream.range(0, listLength).mapToObj(String::valueOf).toArray(String[]::new));

        instance.newJob(dag).join();

        assertEquals(listLength, list.size());
    }

    @Test
    public void when_glob_the_useGlob() throws Exception {
        DAG dag = buildDag("file2.*");

        File file1 = new File(directory, "file1.txt");
        appendToFile(file1, "hello", "world");
        File file2 = new File(directory, "file2.txt");
        appendToFile(file2, "hello2", "world2");

        instance.newJob(dag).join();

        assertEquals(Arrays.asList("hello2", "world2"), new ArrayList<>(list));

        finishDirectory(file1, file2);
    }

    @Test
    public void when_directory_then_ignore() throws Exception {
        DAG dag = buildDag(null);

        File file1 = new File(directory, randomName());
        assertTrue(file1.mkdir());

        instance.newJob(dag).join();

        assertEquals(0, list.size());

        finishDirectory(file1);
    }

    private DAG buildDag(String glob) {
        if (glob == null) {
            glob = "*";
        }

        DAG dag = new DAG();
        Vertex reader = dag.newVertex("reader", readFilesP(directory.getPath(), StandardCharsets.UTF_8, glob))
                .localParallelism(1);
        Vertex writer = dag.newVertex("writer", writeListP(list.getName())).localParallelism(1);
        dag.edge(between(reader, writer));
        return dag;
    }

    private void finishDirectory(File ... files) throws Exception {
        for (File file : files) {
            assertTrue(file.delete());
        }
        assertTrue(directory.delete());
    }
}
