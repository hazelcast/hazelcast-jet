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
 * Identifies the data format of a file to be used as a Jet data source.
 * This is a data object that holds the configuration; actual implementation
 * code is looked up elsewhere, by using this object as a key.
 *
 * @param <T> type of items a source using this file format will emit
 */
public interface FileFormat<T> {

    /**
     * Returns the unique identifier of the file format. The convention is to
     * use the well-known filename suffix or, if there is none, a short-form
     * name of the format.
     */
    String format();


    // Factory methods for supported file formats are here for easy discoverability.

    /**
     * Returns a file format for Avro files.
     */
    static <T> AvroFileFormat<T> avro() {
        return new AvroFileFormat<>();
    }

    /**
     * Returns a file format for Avro files that specifies to use reflection
     * to deserialize the data into instances of the provided Java class.
     */
    static <T> AvroFileFormat<T> avro(Class<T> clazz) {
        return new AvroFileFormat<T>().withReflect(clazz);
    }

    /**
     * Returns a file format for CSV files.
     */
    static <T> CsvFileFormat<T> csv(@Nonnull Class<T> clazz) {
        return new CsvFileFormat<T>(clazz);
    }

    /**
     * Returns a file format for JSON files.
     */
    static <T> JsonFileFormat<T> json(@Nonnull Class<T> clazz) {
        return new JsonFileFormat<>(clazz);
    }

    /**
     * Returns a file format for text files where each line is a {@code String}
     * data item. It uses the UTF-8 character encoding.
     */
    static LinesTextFileFormat lines() {
        return new LinesTextFileFormat();
    }

    /**
     * Returns a file format for text files where each line is a {@code String}
     * data item. This variant allows you to choose the character encoding.
     * Note that the Hadoop-based file connector only accepts UTF-8.
     *
     * @param charset character encoding of the file
     */
    static LinesTextFileFormat lines(@Nonnull Charset charset) {
        return new LinesTextFileFormat(charset);
    }

    /**
     * Returns a file format for Parquet files.
     */
    static <T> ParquetFileFormat<T> parquet() {
        return new ParquetFileFormat<>();
    }

    /**
     * Returns a file format for binary files.
     */
    static RawBytesFileFormat bytes() {
        return new RawBytesFileFormat();
    }

    /**
     * Returns a file format for text files where the whole file is a single
     * string item. It uses the UTF-8 character encoding.
     */
    static TextFileFormat text() {
        return new TextFileFormat();
    }

    /**
     * Returns a file format for text files where the whole file is a single
     * string item. This variant allows you to choose the character encoding.
     * Note that the Hadoop-based file connector only accepts UTF-8.
     *
     * @param charset character encoding of the file
     */
    static TextFileFormat text(@Nonnull Charset charset) {
        return new TextFileFormat(charset);
    }
}
