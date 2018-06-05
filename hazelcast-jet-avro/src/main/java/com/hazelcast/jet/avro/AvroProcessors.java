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
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;

import javax.annotation.Nonnull;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * Static utility class with factory of Apache Avro source processor.
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
            @Nonnull DistributedSupplier<DatumReader<W>> datumReaderSupplier,
            @Nonnull DistributedBiFunction<String, W, R> mapOutputFn,
            boolean sharedFileSystem
    ) {
        return ReadFilesP.metaSupplier(directory, glob,
                path -> uncheckCall(() -> {
                    DataFileReader<W> reader = new DataFileReader<>(path.toFile(), datumReaderSupplier.get());
                    return StreamSupport.stream(reader.spliterator(), false)
                                        .onClose(() -> uncheckRun(reader::close));
                }),
                mapOutputFn, sharedFileSystem);
    }

}
