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

import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.processor.Sinks;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * @see Sinks#writeFile(String, DistributedFunction, Charset, boolean)
 */
public final class WriteFileP {

    private WriteFileP() { }

    /**
     * Use {@link Sinks#writeFile(String, DistributedFunction, Charset, boolean)}
     */
    public static <T> ProcessorSupplier supplier(
            @Nonnull String directoryName,
            @Nonnull DistributedFunction<T, String> toStringF,
            @Nonnull String charset,
            boolean append) {

        return Sinks.writeBuffered(
                globalIndex -> createBufferedWriter(Paths.get(directoryName).resolve(Integer.toString(globalIndex)),
                        charset, append),
                (fileWriter, item) -> uncheckRun(() -> {
                    fileWriter.write(toStringF.apply((T) item));
                    fileWriter.newLine();
                }),
                fileWriter -> uncheckRun(fileWriter::flush),
                fileWriter -> uncheckRun(fileWriter::close)
        );
    }

    private static BufferedWriter createBufferedWriter(Path path, String charset, boolean append) {
        // Ignore the result: we'll fail later when creating the files.
        // It's also false if the directory already existed, which is probable,
        // because we try to create the dir in each processor instance.
        boolean ignored = path.getParent().toFile().mkdirs();

        return uncheckCall(() -> Files.newBufferedWriter(path,
                Charset.forName(charset), StandardOpenOption.CREATE,
                append ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING));
    }

}
