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

import static java.util.Objects.requireNonNull;

/**
 * FileFormat for CSV files
 *
 * @param <T> type of items emitted from the source
 */
public class CsvFileFormat<T> implements FileFormat<T> {

    /**
     * Format id for CSV
     */
    public static final String FORMAT_CSV = "csv";

    private final Class<T> clazz;

    /**
     * Jet will deserialize the items into an instance of the given class.
     * It will use the first line of the CSV (header) to infer the column names
     * to map to the fields.
     *
     * @param clazz class to deserialize into
     */
    public CsvFileFormat(Class<T> clazz) {
        this.clazz = requireNonNull(clazz, "clazz must not be null");
    }


    /**
     * Class to deserialize into
     */
    public Class<T> clazz() {
        return clazz;
    }

    @Override
    public String format() {
        return FORMAT_CSV;
    }
}
