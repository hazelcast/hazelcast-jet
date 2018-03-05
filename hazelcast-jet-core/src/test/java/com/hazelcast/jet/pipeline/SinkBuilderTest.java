/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SinkBuilderTest extends PipelineTestSupport {


    @Test
    public void test_sink_builder_file_sink() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        addToSrcList(input);
        String pathListName = randomName();

        // When
        BatchStage<Integer> batchStage = pipeline.drawFrom(Sources.list(srcName));
        SinkBuilder<Integer, File> builder = batchStage.sinkBuilder();

        builder.createFn((jetInstance) ->
                uncheckCall(() -> {
                    File directory = createTempDirectory();
                    File file = new File(directory, randomName());
                    assertTrue(file.createNewFile());
                    jetInstance.getList(pathListName).add(directory.toPath().toString());
                    return file;
                }))
               .addItemFn((sink, item) -> uncheckRun(() -> {
                   appendToFile(sink, item.toString());
               }));
        Sink<Integer> sink = builder.build();
        batchStage.drainTo(sink);
        execute();

        //then
        assertTrueEventually(() -> {
            List<String> paths = new ArrayList<>(jet().<String>getList(pathListName));
            long count = paths.stream().map(Paths::get)
                              .flatMap(path -> uncheckCall(() -> Files.list(path)))
                              .flatMap(path -> uncheckCall(() -> Files.readAllLines(path).stream()))
                              .count();
            assertEquals(ITEM_COUNT, count);
        });
    }

    @Test
    public void test_sink_builder_socket_sink() throws IOException {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        addToSrcList(input);

        AtomicInteger counter = new AtomicInteger();
        ServerSocket serverSocket = new ServerSocket(0);
        spawn(() -> uncheckRun(() -> {
            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                spawn(() -> uncheckRun(() -> {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                        while (reader.readLine() != null) {
                            counter.incrementAndGet();
                        }
                    }
                }));
            }
        }));

        // When
        BatchStage<Integer> batchStage = pipeline.drawFrom(Sources.list(srcName));
        SinkBuilder<Integer, BufferedWriter> builder = batchStage.sinkBuilder();
        final int localPort = serverSocket.getLocalPort();
        builder.createFn((jetInstance) -> uncheckCall(() -> {
                    OutputStream outputStream = new Socket("localhost", localPort).getOutputStream();
                    OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, UTF_8);
                    return new BufferedWriter(outputStreamWriter);
                }
                )
        )
               .addItemFn((sink, item) -> uncheckRun(() -> {
                           sink.write(item);
                           sink.write('\n');
                       })
               )
               .flushFn(sink -> uncheckRun(sink::flush))
               .destroyFn(sink -> uncheckRun(sink::close));
        Sink<Integer> sink = builder.build();
        batchStage.drainTo(sink);
        execute();

        //then
        try {
            assertTrueEventually(() -> assertEquals(ITEM_COUNT, counter.get()));
        } finally {
            serverSocket.close();
        }
    }
}
