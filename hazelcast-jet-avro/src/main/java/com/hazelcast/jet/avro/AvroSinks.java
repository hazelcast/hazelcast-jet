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

import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * Contains factory methods for Avro sinks.
 */
public final class AvroSinks {

    private AvroSinks() {
    }

    /**
     * Returns a sink that that writes the items it receives to avro files. Each
     * processor will write to its own file whose name is equal to the
     * processor's global index (an integer unique to each processor of the
     * vertex), but a single pathname is used to resolve the containing
     * directory of all files, on all cluster members.
     * <p>
     * The sink creates a {@link DataFileWriter} for each processor using the
     * supplied {@code datumWriterSupplier} with the given {@link Schema}.
     * <p>
     * No state is saved to snapshot for this sink. After the job is restarted,
     * the items will likely be duplicated, providing an <i>at-least-once</i>
     * guarantee.
     * <p>
     * The default local parallelism for this sink is 1.
     *
     * @param directoryName directory to create the files in. Will be created
     *                      if it doesn't exist. Must be the same on all members.
     * @param schemaSupplier the record schema supplier
     * @param datumWriterSupplier the record writer supplier
     * @param <R> the type of the record
     */
    @Nonnull
    public static <R> Sink<R> files(
            @Nonnull String directoryName,
            @Nonnull DistributedSupplier<Schema> schemaSupplier,
            @Nonnull DistributedSupplier<DatumWriter<R>> datumWriterSupplier
    ) {
        return Sinks.<DataFileWriter<R>, R>builder("avroFilesSink(" + directoryName + ')',
                context -> createWriter(Paths.get(directoryName), context.globalProcessorIndex(),
                        schemaSupplier, datumWriterSupplier))
                .onReceiveFn((writer, item) -> uncheckRun(() -> writer.append(item)))
                .flushFn(writer -> uncheckRun(writer::flush))
                .destroyFn(writer -> uncheckRun(writer::close))
                .preferredLocalParallelism(1)
                .build();
    }

    /**
     * Convenience for {@link #files(String, DistributedSupplier,
     * DistributedSupplier)} which uses either {@link SpecificDatumWriter} or
     * {@link ReflectDatumWriter} depending on the supplied {@code recordClass}
     */
    @Nonnull
    public static <R> Sink<R> files(
            @Nonnull String directoryName,
            @Nonnull DistributedSupplier<Schema> schemaSupplier,
            @Nonnull Class<R> recordClass
    ) {
        return files(directoryName, schemaSupplier, () -> SpecificRecord.class.isAssignableFrom(recordClass) ?
                new SpecificDatumWriter<>(recordClass) : new ReflectDatumWriter<>(recordClass));
    }

    /**
     * Convenience for {@link #files(String, DistributedSupplier,
     * DistributedSupplier)} which uses {@link GenericDatumWriter}
     */
    @Nonnull
    public static Sink<IndexedRecord> files(
            @Nonnull String directoryName,
            @Nonnull DistributedSupplier<Schema> schemaSupplier
    ) {
        return files(directoryName, schemaSupplier, GenericDatumWriter::new);
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
