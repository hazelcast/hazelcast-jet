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

import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nonnull;
import java.io.File;

/**
 * Contains factory methods for Avro sources.
 */
public final class AvroSources {

    private AvroSources() {
    }

    /**
     * Returns a source that reads records from Apache Avro file and emits the
     * results of transforming each record with the supplied mapping function.
     * <p>
     * The source assigns files in the supplied directory to Jet {@link
     * com.hazelcast.jet.core.Processor processors} according to the {@code
     * sharedFileSystem} parameter.
     * <p>
     * Any {@code IOException} will cause the job to fail.
     * <p>
     * Default local parallelism for this processor is 2 (or less if less CPUs
     * are available).
     * <p>
     * This source does not save any state to snapshot. If the job is restarted,
     * all entries will be emitted again.
     *
     * @param directory           parent directory of the files
     * @param glob                the globbing mask, see {@link
     *                            java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     *                            Use {@code "*"} for all files.
     * @param datumReaderSupplier the datum reader supplier
     * @param mapOutputFn         function to create output items. Parameters are
     *                            {@code fileName} and {@code W}.
     * @param sharedFileSystem    true if files are shared across cluster false if
     *                            files are local
     * @param <W>                 the type of the records
     * @param <R>                 the type of the emitted value
     */
    @Nonnull
    public static <W, R> BatchSource<R> files(
            @Nonnull String directory,
            @Nonnull String glob,
            @Nonnull DistributedSupplier<DatumReader<W>> datumReaderSupplier,
            @Nonnull DistributedBiFunction<String, W, ? extends R> mapOutputFn,
            boolean sharedFileSystem
    ) {
        return Sources.batchFromProcessor("avroFilesSource(" + new File(directory, glob) + ')',
                AvroProcessors.readFilesP(directory, glob, datumReaderSupplier, mapOutputFn, sharedFileSystem));
    }

    /**
     * Convenience for {@link
     * #files(String, String, DistributedSupplier, DistributedBiFunction, boolean)}
     * which reads all the files in the supplied directory as specific records
     * using supplied {@code recordClass}. If {@code recordClass} implements
     * {@link SpecificRecord}, {@link SpecificDatumReader} is used otherwise
     * {@link ReflectDatumReader} to read the records.
     */
    @Nonnull
    public static <R> BatchSource<R> files(
            @Nonnull String directory,
            @Nonnull Class<R> recordClass,
            boolean sharedFileSystem) {
        return files(directory, "*", () -> SpecificRecord.class.isAssignableFrom(recordClass) ?
                        new SpecificDatumReader<>(recordClass) : new ReflectDatumReader<>(recordClass),
                (s, r) -> r, sharedFileSystem);
    }

    /**
     * Convenience for {@link
     * #files(String, String, DistributedSupplier, DistributedBiFunction, boolean)}
     * which reads all the files in the supplied directory as generic records and
     * emits the results of transforming each generic record with the supplied
     * mapping function.
     */
    @Nonnull
    public static <R> BatchSource<R> files(
            @Nonnull String directory,
            @Nonnull DistributedBiFunction<String, GenericRecord, R> mapOutputFn,
            boolean sharedFileSystem) {
        return files(directory, "*", GenericDatumReader::new, mapOutputFn, sharedFileSystem);
    }

}
