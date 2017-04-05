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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * @see com.hazelcast.jet.Processors#writeFile(String, Charset, boolean, boolean)
 */
public final class WriteFileP {

    private WriteFileP() { }

    public static ProcessorSupplier supplier(String fileName, String charset, boolean append, boolean flushEarly) {
        return new ProcessorSupplier() {
            static final long serialVersionUID = 1L;

            @Nonnull
            @Override
            public Collection<? extends Processor> get(int count) {
                if (count > 1) {
                    throw new JetException(WriteFileP.class.getSimpleName() + " must have localParallelism=1");
                }
                // create the processors
                return Collections.singletonList(new WriteBufferedP<>(
                        () -> createBufferedWriter(fileName, charset, append),
                        (writer, item) -> uncheckRun(() -> {
                            writer.write(item.toString());
                            writer.newLine();
                        }),
                        flushEarly ? writer -> uncheckRun(writer::flush) : writer -> {
                        },
                        bufferedWriter -> uncheckRun(bufferedWriter::close)
                ));
            }
        };
    }

    private static BufferedWriter createBufferedWriter(String fileName, String charset, boolean append) {
        return uncheckCall(() -> Files.newBufferedWriter(Paths.get(fileName),
                charset == null ? StandardCharsets.UTF_8 : Charset.forName(charset),
                append ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING));
    }

}
