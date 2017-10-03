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

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.stream.Collectors.toList;

/**
 * @see SourceProcessors#streamSocket(String, int, Charset)
 */
public final class StreamSocketP extends AbstractProcessor implements Closeable {

    private final String host;
    private final int port;
    private final Charset charset;
    private BufferedReader bufferedReader;
    private String pendingLine;

    private StreamSocketP(String host, int port, Charset charset) {
        this.host = host;
        this.port = port;
        this.charset = charset;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        getLogger().info("Connecting to socket " + hostAndPort());
        Socket socket = new Socket(host, port);
        getLogger().info("Connected to socket " + hostAndPort());
        bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
    }

    @Override
    public boolean complete() {
        return uncheckCall(this::tryComplete);
    }

    private boolean tryComplete() throws IOException {
        if (pendingLine == null) {
            pendingLine = bufferedReader.readLine();
            if (pendingLine == null) {
                return true;
            }
        }
        boolean success = tryEmit(pendingLine);
        if (success) {
            pendingLine = null;
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        if (bufferedReader != null) {
            getLogger().info("Closing socket " + hostAndPort());
            bufferedReader.close();
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private String hostAndPort() {
        return host + ':' + port;
    }

    /**
     * Internal API, use {@link SourceProcessors#streamSocket(String, int, Charset)}.
     */
    public static ProcessorSupplier supplier(String host, int port, @Nonnull String charset) {
        return new Supplier(host, port, charset);
    }

    private static final class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String host;
        private final int port;
        private final String charset;
        private transient List<StreamSocketP> processors;

        private Supplier(@Nonnull String host, int port, @Nonnull String charset) {
            this.host = host;
            this.port = port;
            this.charset = charset;
        }

        @Override @Nonnull
        public List<? extends Processor> get(int count) {
            processors = IntStream.range(0, count)
                                  .mapToObj(i -> new StreamSocketP(host, port, Charset.forName(charset)))
                                  .collect(toList());
            return processors;
        }

        @Override
        public void complete(Throwable error) {
            processors.forEach(p -> uncheckRun(p::close));
        }
    }
}
