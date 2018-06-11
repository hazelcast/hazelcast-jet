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

package com.hazelcast.jet.avro;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.impl.connector.WriteBufferedP;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * Static utility class with factories of Apache Avro source and sink
 * processors.
 */
public final class AvroProcessors {

    private AvroProcessors() {
    }

    /**
     * Returns a supplier of processors for {@link AvroSources#files}.
     */
    @Nonnull
    public static <W, R> ProcessorMetaSupplier readFilesP(
            @Nonnull String directory,
            @Nonnull String glob,
            boolean sharedFileSystem,
            @Nonnull DistributedSupplier<DatumReader<W>> datumReaderSupplier,
            @Nonnull DistributedBiFunction<String, W, R> mapOutputFn
    ) {
        return ReadFilesP.metaSupplier(directory, glob, sharedFileSystem,
                path -> uncheckCall(() -> {
                    DataFileReader<W> reader = new DataFileReader<>(path.toFile(), datumReaderSupplier.get());
                    return StreamSupport.stream(reader.spliterator(), false)
                                        .onClose(() -> uncheckRun(reader::close));
                }),
                mapOutputFn);
    }

    /**
     * Returns a supplier of processors for {@link AvroSinks#files}.
     */
    @Nonnull
    public static <R> ProcessorMetaSupplier writeFilesP(
            @Nonnull String directoryName,
            @Nonnull DistributedSupplier<Schema> schemaSupplier,
            @Nonnull DistributedSupplier<DatumWriter<R>> datumWriterSupplier
    ) {
        return ProcessorMetaSupplier.of(
                WriteBufferedP.<DataFileWriter<R>, R>supplier(
                        context -> createWriter(Paths.get(directoryName), context.globalProcessorIndex(),
                                schemaSupplier, datumWriterSupplier),
                        (writer, item) -> uncheckRun(() -> writer.append(item)),
                        writer -> uncheckRun(writer::flush),
                        writer -> uncheckRun(writer::close)
                ), 1);
    }

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
            justification = "mkdirs() returns false if the directory already existed, which is good. "
                    + "We don't care even if it didn't exist and we failed to create it, "
                    + "because we'll fail later when trying to create the file.")
    private static <R> DataFileWriter<R> createWriter(Path directory, int globalIndex,
                                                      DistributedSupplier<Schema> schemaSupplier,
                                                      DistributedSupplier<DatumWriter<R>> datumWriterSupplier) {
        directory.toFile().mkdirs();

        Path file = directory.resolve(String.valueOf(globalIndex));

        DataFileWriter<R> writer = new DataFileWriter<>(datumWriterSupplier.get());
        uncheckRun(() -> writer.create(schemaSupplier.get(), file.toFile()));
        return writer;
    }

}
