/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static com.hazelcast.jet.core.processor.SinkProcessors.writeFileP;

/**
 * See {@link Sinks#filesBuilder}.
 *
 * @param <T> type of the items the sink accepts
 *
 * @since 3.0
 */
public final class FileSinkBuilder<T> {

    private final String directoryName;

    private FunctionEx<? super T, String> toStringFn = Object::toString;
    private Charset charset = StandardCharsets.UTF_8;
    private boolean append;
    private String datePattern;
    private Long maxFileSize;

    /**
     * Use {@link Sinks#filesBuilder}.
     */
    FileSinkBuilder(@Nonnull String directoryName) {
        this.directoryName = directoryName;
    }

    /**
     * Sets the function which converts the item to its string representation.
     * Each item is followed with a platform-specific line separator. Default
     * value is {@link Object#toString}.
     */
    public FileSinkBuilder<T> toStringFn(@Nonnull FunctionEx<? super T, String> toStringFn) {
        this.toStringFn = toStringFn;
        return this;
    }

    /**
     * Sets the character set used to encode the files. Default value is {@link
     * java.nio.charset.StandardCharsets#UTF_8}.
     */
    public FileSinkBuilder<T> charset(@Nonnull Charset charset) {
        this.charset = charset;
        return this;
    }

    /**
     * Sets whether to append ({@code true}) or overwrite ({@code false})
     * an existing file. Default value is {@code false}.
     * <p>
     * The value is ignored if:
     * <ul>
     *     <li>{@linkplain JobConfig#setProcessingGuarantee processing
     *     guarantee} is {@linkplain ProcessingGuarantee#EXACTLY_ONCE
     *     exactly-once}
     *
     *     <li>you use {@linkplain #rollByFileSize(Long) rolling by file size}
     * </ul>
     *
     * In either of the above cases we don't overwrite any files as they are
     * intended for streaming use cases; the file name contains a sequence
     * number and a new file is always used.
     */
    public FileSinkBuilder<T> append(boolean append) {
        this.append = append;
        return this;
    }

    /**
     * Sets a date pattern that will be included in the file name. Each time
     * the formatted current time changes a new file will be started, therefore
     * the highest-resolution element included in the pattern determines the
     * rolling interval. For example, if the {@code datePattern} is {@code
     * yyyy-MM-dd}, the file will roll over every day.
     * <p>
     * Since this option is typically used in streaming use cases, the {@link
     * #append} option automatically enabled so that the file is not
     * overwritten in case of a job restart.
     * <p>
     * The rolling is based on system time, not on event time. By default no
     * rolling by date is done.
     */
    public FileSinkBuilder<T> rollByDate(@Nullable String datePattern) {
        this.datePattern = datePattern;
        this.append = true;
        return this;
    }

    /**
     * Enables rolling by file size. If the size after writing a batch of items
     * exceeds the limit, a new file will be started. From this follows that
     * the file will typically be larger than the given maximum.
     */
    public FileSinkBuilder<T> rollByFileSize(@Nullable Long maxFileSize) {
        this.maxFileSize = maxFileSize;
        return this;
    }

    /**
     * Creates and returns the file {@link Sink} with the supplied components.
     */
    public Sink<T> build() {
        return Sinks.fromProcessor("filesSink(" + directoryName + ')',
                writeFileP(directoryName, toStringFn, charset, append, datePattern, maxFileSize));
    }
}
