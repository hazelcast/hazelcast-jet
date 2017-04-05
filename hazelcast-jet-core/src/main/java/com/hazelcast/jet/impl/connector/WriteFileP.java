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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * @see com.hazelcast.jet.Processors#writeFile(String, Charset, boolean, boolean)
 */
public class WriteFileP implements Processor, Closeable {

    private final Path file;
    private final Charset charset;
    private final boolean append;
    private final boolean flushEarly;

    private Writer writer;

    WriteFileP(String file, Charset charset, boolean append, boolean flushEarly) {
        this.file = Paths.get(file);
        this.charset = charset;
        this.append = append;
        this.flushEarly = flushEarly;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        try {
            writer = Files.newBufferedWriter(file, charset, append ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        try {
            for (Object item; (item = inbox.poll()) != null; ) {
                writer.write(item.toString());
                writer.write('\n');
            }
            if (flushEarly) {
                writer.flush();
            }
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
        writer = null;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    /**
     * @see com.hazelcast.jet.Processors#writeFile(String, Charset, boolean, boolean)
     */
    @Nonnull
    public static ProcessorSupplier supplier(String file, String charset, boolean append, boolean flushEarly) {
        return new Supplier(file, charset, append, flushEarly);
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;
        private final String file;
        private final String charset;
        private final boolean append;
        private final boolean flushEarly;

        private transient WriteFileP processor;

        Supplier(String file, String charset, boolean append, boolean flushEarly) {
            this.file = file;
            this.charset = charset;
            this.append = append;
            this.flushEarly = flushEarly;
        }

        @Override @Nonnull
        public List<WriteFileP> get(int count) {
            if (count != 1) {
                throw new JetException("localParallelism must be 1 for " + WriteFileP.class.getSimpleName());
            }
            processor = new WriteFileP(file, charset == null ? StandardCharsets.UTF_8 : Charset.forName(charset), append, flushEarly);
            return Collections.singletonList(processor);
        }

        @Override
        public void complete(Throwable error) {
            try {
                processor.close();
            } catch (IOException e) {
                throw sneakyThrow(e);
            }
        }
    }
}
