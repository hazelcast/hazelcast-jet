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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * FileFormat for text files which read each line as a String item emitted from the source
 */
public class LinesTextFileFormat implements FileFormat<String> {

    /**
     * Format id for text file as lines
     */
    public static final String FORMAT_LINES = "lines";

    private final Charset charset;

    /**
     * Create LinesTextFileFormat with default character encoding (UTF-8)
     */
    public LinesTextFileFormat() {
        this(UTF_8);
    }

    /**
     * Create LinesTextFileFormat with default character encoding (UTF-8)
     *
     * NOTE: This option is supported for local files only, not for files read using the Hadoop connector
     */
    public LinesTextFileFormat(@Nonnull Charset charset) {
        this.charset = requireNonNull(charset, "charset must not be null");
    }

    /**
     * The configured character encoding
     */
    public Charset charset() {
        return charset;
    }

    @Override
    public String format() {
        return FORMAT_LINES;
    }
}
