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

import com.hazelcast.jet.ProcessorMetaSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.DistributedFunctions.noopConsumer;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * @see com.hazelcast.jet.Processors#writeFile(String, Charset, boolean, boolean)
 */
public final class WriteFileP {

    private WriteFileP() { }

    public static ProcessorMetaSupplier supplier(@Nonnull String fileNamePrefix, @Nullable String fileNameSuffix,
            @Nullable String charset, boolean append, boolean flushEarly) {
        return addresses -> address -> {
            // need to do this, as Address is not serializable
            String sAddress = sanitizeAddressForFilename(address.toString());

            return count -> IntStream.range(0, count)
                    .mapToObj(index -> new WriteBufferedP<>(
                            () -> createBufferedWriter(
                                    createFileName(fileNamePrefix, fileNameSuffix, sAddress, index),
                                    charset, append),
                            (writer, item) -> uncheckRun(() -> {
                                writer.write(item.toString());
                                writer.newLine();
                            }),
                            flushEarly ? writer -> uncheckRun(writer::flush) : noopConsumer(),
                            bufferedWriter -> uncheckRun(bufferedWriter::close)
                    )).collect(Collectors.toList());
        };
    }

    static String createFileName(@Nonnull String fileNamePrefix, @Nullable String fileNameSuffix, String sAddress, int index) {
        return fileNamePrefix + "-" + sAddress + "#" + index + fileNameSuffix;
    }

    static String sanitizeAddressForFilename(String str) {
        return str.replace(':', '_');
    }

    private static BufferedWriter createBufferedWriter(String fileName, String charset, boolean append) {
        return uncheckCall(() -> Files.newBufferedWriter(Paths.get(fileName),
                charset == null ? StandardCharsets.UTF_8 : Charset.forName(charset), StandardOpenOption.CREATE,
                append ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING));
    }

}
