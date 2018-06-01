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

    @Nonnull
    public static <R> Sink<R> files(
            @Nonnull String directoryName,
            @Nonnull DistributedSupplier<Schema> schemaSupplier,
            @Nonnull DistributedSupplier<DatumWriter<R>> datumWriterSupplier
    ) {
        return Sinks.<DataFileWriter<R>, R>builder(context -> createWriter(Paths.get(directoryName),
                context.globalProcessorIndex(), schemaSupplier, datumWriterSupplier))
                .onReceiveFn((writer, item) -> uncheckRun(() -> writer.append(item)))
                .flushFn(writer -> uncheckRun(writer::flush))
                .destroyFn(writer -> uncheckRun(writer::close))
                .preferredLocalParallelism(1)
                .build();
    }

    @Nonnull
    public static <R> Sink<R> files(
            @Nonnull String directoryName,
            @Nonnull DistributedSupplier<Schema> schemaSupplier,
            @Nonnull Class<R> recordClass
    ) {
        return files(directoryName, schemaSupplier, () -> SpecificRecord.class.isAssignableFrom(recordClass) ?
                new SpecificDatumWriter<>(recordClass) : new ReflectDatumWriter<>(recordClass));
    }

    @Nonnull
    public static Sink<IndexedRecord> files(
            @Nonnull String directoryName,
            @Nonnull DistributedSupplier<Schema> schemaSupplier
    ) {
        return files(directoryName, schemaSupplier, GenericDatumWriter::new);
    }

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
