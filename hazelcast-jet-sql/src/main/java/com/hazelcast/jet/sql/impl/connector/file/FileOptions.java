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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.sql.impl.QueryException;

import java.util.Map;

import static com.hazelcast.jet.sql.SqlConnector.TO_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_CHARSET;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_DELIMITER;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_GLOB;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_HEADER;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_PATH;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_SHARED_FILE_SYSTEM;
import static java.nio.charset.StandardCharsets.UTF_8;

final class FileOptions {

    private final Map<String, String> options;

    private FileOptions(Map<String, String> options) {
        if (options.get(TO_SERIALIZATION_FORMAT) == null) {
            throw QueryException.error("Missing '" + TO_SERIALIZATION_FORMAT + "' option");
        }
        if (options.get(TO_PATH) == null) {
            throw QueryException.error("Missing '" + TO_PATH + "' option");
        }

        this.options = options;
    }

    public static FileOptions from(Map<String, String> options) {
        return new FileOptions(options);
    }

    String format() {
        return options.get(TO_SERIALIZATION_FORMAT);
    }

    String path() {
        return options.get(TO_PATH);
    }

    String glob() {
        return options.getOrDefault(TO_GLOB, "*");
    }

    boolean sharedFileSystem() {
        return Boolean.parseBoolean(options.getOrDefault(TO_SHARED_FILE_SYSTEM, "true"));
    }

    String charset() {
        return options.getOrDefault(TO_CHARSET, UTF_8.name());
    }

    String delimiter() {
        return options.getOrDefault(TO_DELIMITER, ",");
    }

    boolean header() {
        return Boolean.parseBoolean(options.getOrDefault(TO_HEADER, "false"));
    }
}
