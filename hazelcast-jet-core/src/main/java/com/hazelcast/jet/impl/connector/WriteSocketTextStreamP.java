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

import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.List;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

/**
 * A writer that connects to a specified socket and writes the items as text.
 * This processor expects a server-side socket to be available for connecting to
 * <p>
 * Each processor instance will create a socket connection to the configured [host:port]
 */
public class WriteSocketTextStreamP implements Processor, Closeable {

    private final String host;
    private final int port;

    private ILogger logger;
    private Socket socket;
    private BufferedWriter bufferedWriter;

    WriteSocketTextStreamP(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Creates a supplier for {@link WriteSocketTextStreamP}
     *
     * @param host The host name to connect to
     * @param port The port number to connect to
     */
    public static ProcessorSupplier supplier(String host, int port) {
        return new Supplier(host, port);
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        logger = context.logger();
        try {
            logger.info("Connecting to socket " + hostAndPort());
            socket = new Socket(host, port);
            logger.info("Connected to socket " + hostAndPort());
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        try {
            for (Object item; (item = inbox.poll()) != null; ) {
                bufferedWriter.write(item.toString());
            }
            bufferedWriter.flush();
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public boolean complete() {
        uncheckRun(this::close);
        return true;
    }

    @Override
    public void close() throws IOException {
        if (bufferedWriter != null) {
            logger.info("Closing socket " + hostAndPort());
            bufferedWriter.close();
            socket.close();
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private String hostAndPort() {
        return host + ':' + port;
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String host;
        private final int port;
        private transient List<Processor> processors;

        Supplier(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            processors = range(0, count)
                    .mapToObj(i -> new WriteSocketTextStreamP(host, port))
                    .collect(toList());
            return processors;
        }

        @Override
        public void complete(Throwable error) {
            processors.forEach(p -> uncheckRun(((WriteSocketTextStreamP) p)::close));
        }
    }

}
