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

package com.hazelcast.jet.pipeline.file.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.file.FileFormat;

import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Provides a mapping function from a Path to a Stream of items emitted from local filesystem source
 */
public interface ReadFileFnProvider {

    /**
     * Returns a mapping function for a given a configured FileFormat
     *
     * @param format FileFormat
     *
     * @return mapping function, which maps Path on local filesystem to a stream of
     * items emitted from the source
     */
    <T> FunctionEx<Path, Stream<T>> createReadFileFn(FileFormat<T> format);

    /**
     * Return unique identifier of the FileFormat, which the mapFn can read
     */
    String format();

}
