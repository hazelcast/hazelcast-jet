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

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_CHARSET;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_DELIMITER;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_GLOB;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_HEADER;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_PATH;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_SHARED_FILE_SYSTEM;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_S3_ACCESS_KEY;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_S3_SECRET_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;

final class FileOptions {

    private final Map<String, String> options;

    private FileOptions(Map<String, String> options) {
        if (options.get(OPTION_SERIALIZATION_FORMAT) == null) {
            throw QueryException.error("Missing '" + OPTION_SERIALIZATION_FORMAT + "' option");
        }
        if (options.get(OPTION_PATH) == null) {
            throw QueryException.error("Missing '" + OPTION_PATH + "' option");
        }

        this.options = options;
    }

    public static FileOptions from(Map<String, String> options) {
        return new FileOptions(options);
    }

    String format() {
        return options.get(OPTION_SERIALIZATION_FORMAT);
    }

    String path() {
        return options.get(OPTION_PATH);
    }

    String glob() {
        return options.getOrDefault(OPTION_GLOB, "*");
    }

    boolean sharedFileSystem() {
        return Boolean.parseBoolean(options.getOrDefault(OPTION_SHARED_FILE_SYSTEM, "true"));
    }

    String charset() {
        return options.getOrDefault(OPTION_CHARSET, UTF_8.name());
    }

    String delimiter() {
        return options.getOrDefault(OPTION_DELIMITER, ",");
    }

    boolean header() {
        return Boolean.parseBoolean(options.getOrDefault(OPTION_HEADER, "false"));
    }

    String s3AccessKey() {
        String value = options.get(OPTION_S3_ACCESS_KEY);
        if (value == null) {
            throw QueryException.error("Missing '" + OPTION_S3_ACCESS_KEY + "' option");
        }
        return value;
    }

    String s3SecretKey() {
        String value = options.get(OPTION_S3_SECRET_KEY);
        if (value == null) {
            throw QueryException.error("Missing '" + OPTION_S3_SECRET_KEY + "' option");
        }
        return value;
    }
}
