/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline.file;

import javax.annotation.Nonnull;
import java.nio.charset.Charset;

/**
 * Factory methods for {@link FileFormat}s discoverability
 */
public final class FileFormats {

    /**
     * Helper class
     */
    private FileFormats() {
    }

    /**
     * File format for Avro files, see {@link AvroFileFormat}
     */
    @Nonnull
    public static <T> AvroFileFormat<T> avro() {
        return new AvroFileFormat<>();
    }

    /**
     * File format for AvroFiles, see {@link AvroFileFormat}
     */
    @Nonnull
    public static <T> AvroFileFormat<T> avro(@Nonnull Class<T> clazz) {
        return new AvroFileFormat<T>().withReflect(clazz);
    }

    /**
     * File format for CSV files, see {@link CsvFileFormat}
     */
    @Nonnull
    public static <T> CsvFileFormat<T> csv(@Nonnull Class<T> clazz) {
        return new CsvFileFormat<T>(clazz);
    }

    /**
     * File format for JSON files, see {@link JsonFileFormat}
     */
    @Nonnull
    public static <T> JsonFileFormat<T> json(@Nonnull Class<T> clazz) {
        return new JsonFileFormat<>(clazz);
    }

    /**
     * File format for text files where each lines is emitted as
     * a String from the source
     */
    @Nonnull
    public static LinesTextFileFormat lines() {
        return new LinesTextFileFormat();
    }

    /**
     * File format for text files where each lines is emitted as
     * a String from the source, see {@link LinesTextFileFormat}
     *
     * @param charset charset of the file, not supported by Hadoop based file connector, which uses only UTF-8
     */
    @Nonnull
    public static LinesTextFileFormat lines(@Nonnull Charset charset) {
        return new LinesTextFileFormat(charset);
    }

    /**
     * File format for Parquet files, see {@link ParquetFileFormat}
     */
    @Nonnull
    public static <T> ParquetFileFormat<T> parquet() {
        return new ParquetFileFormat<>();
    }

    /**
     * File format for binary files, see {@link RawBytesFileFormat}
     */
    @Nonnull
    public static RawBytesFileFormat bytes() {
        return new RawBytesFileFormat();
    }

    /**
     * File format for text files, where the whole file is emitted as
     * a single string, see {@link TextFileFormat}
     */
    @Nonnull
    public static TextFileFormat text() {
        return new TextFileFormat();
    }

    /**
     * File format for text files, where the whole file is emitted as
     * a single string, see {@link TextFileFormat}
     *
     * @param charset charset of the file, not supported by Hadoop based file connector, which uses only UTF-8
     */
    @Nonnull
    public static TextFileFormat text(@Nonnull Charset charset) {
        return new TextFileFormat(charset);
    }
}
