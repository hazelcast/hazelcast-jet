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
import javax.annotation.Nullable;
import java.util.List;

/**
 * {@link FileFormat} for CSV files. See {@link FileFormat#csv} for more
 * details.
 *
 * @param <T> type of items a source using this file format will emit
 * @since 4.4
 */
public class CsvFileFormat<T> implements FileFormat<T> {

    /**
     * Format ID for CSV.
     */
    public static final String FORMAT_CSV = "csv";

    private Class<T> clazz;
    private List<String> stringArrayFieldList;

    /**
     * Creates {@link CsvFileFormat}. See {@link FileFormat#csv} for more
     * details.
     */
    CsvFileFormat() {
    }

    /**
     * Specifies class that data will be deserialized into.
     * If parameter is {@code null} data is deserialized into
     * {@code Map<String, String>}.
     *
     * @param clazz type of the object to deserialize CSV into
     */
    @Nonnull
    public CsvFileFormat<T> withClass(@Nullable Class<T> clazz) {
        this.clazz = clazz;
        return this;
    }

    /**
     * This setting is only applied when the class (as set with {@link
     * #withClass} is {@code String[]}. It specifies which column should be at
     * which index in the resulting string array. It is useful if the files
     * have different field order or don't have the same set of columns.
     * <p>
     * For example, if the argument is {@code [surname, name]}, then the format
     * will always return items of type String[2] where at index 0 is the
     * {@code surname} column and at index 1 is the {@code name} column,
     * regardless of the actual columns found in a particular file. If some
     * file doesn't have some field, the value at its index will always be 0.
     * <p>
     * If the given list is {@code null}, the length and order of the string
     * array will match the order found in each file. It can be different for
     * each file. If it's an empty array, a zero-length array will be returned.
     *
     * @param fieldList list of fields in the desired order for {@code
     *      String[]} class
     */
    @Nonnull
    public CsvFileFormat<T> withStringArrayFieldList(@Nullable List<String> fieldList) {
        this.stringArrayFieldList = fieldList;
        return this;
    }

    /**
     * Returns the class Jet will deserialize data into.
     * Null if not set.
     */
    @Nullable
    public Class<T> clazz() {
        return clazz;
    }

    @Nonnull
    @Override
    public String format() {
        return FORMAT_CSV;
    }

    /**
     * Return the desired list of fields that is used with {@code String[]}
     * class.
     */
    @Nullable
    public List<String> stringArrayFieldList() {
        return stringArrayFieldList;
    }
}
