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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.impl.util.IOUtil;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.JsonFileFormat;
import com.hazelcast.jet.pipeline.file.LinesTextFileFormat;
import com.hazelcast.jet.pipeline.file.ParquetFileFormat;
import com.hazelcast.jet.pipeline.file.RawBytesFileFormat;
import com.hazelcast.jet.pipeline.file.TextFileFormat;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.pipeline.Sources.batchFromProcessor;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of FileSourceFactory for the local filesystem.
 */
public class LocalFileSourceFactory implements FileSourceFactory {

    private static Map<String, ReadFileFnProvider> readFileFnProviders;

    static {
        Map<String, ReadFileFnProvider> mapFns = new HashMap<>();

        addMapFnProvider(mapFns, new JsonReadFileFnProvider());
        addMapFnProvider(mapFns, new LinesReadFileFnProvider());
        addMapFnProvider(mapFns, new ParquetReadFileFnProvider());
        addMapFnProvider(mapFns, new RawBytesReadFileFnProvider());
        addMapFnProvider(mapFns, new TextReadFileFnProvider());

        ServiceLoader<ReadFileFnProvider> loader = ServiceLoader.load(ReadFileFnProvider.class);
        for (ReadFileFnProvider readFileFnProvider : loader) {
            addMapFnProvider(mapFns, readFileFnProvider);
        }

        LocalFileSourceFactory.readFileFnProviders = Collections.unmodifiableMap(mapFns);
    }

    private static void addMapFnProvider(Map<String, ReadFileFnProvider> mapFns, ReadFileFnProvider provider) {
        mapFns.put(provider.format(), provider);
    }

    @Nonnull @Override
    public <T> BatchSource<T> create(@Nonnull FileSourceBuilder<T> builder) {

        // Avoid serialization of the builder
        String path = builder.path();
        FileFormat<T> format = requireNonNull(builder.format());
        String formatId = format.format();
        boolean sharedFileSystem = builder.isSharedFileSystem();

        return batchFromProcessor("filesSource(" + new File(builder.path()) + ')',
                ProcessorMetaSupplier.of(2, () -> {
                    Tuple2<String, String> dirAndGlob = deriveDirectoryAndGlobFromPath(path);
                    assert dirAndGlob.f0() != null && dirAndGlob.f1() != null;

                    ReadFileFnProvider readFileFnProvider = readFileFnProviders.get(formatId);
                    if (readFileFnProvider == null) {
                        throw new JetException("Could not find ReadFileFnProvider for FileFormat: " + formatId + ". " +
                                "Did you provide correct modules on classpath?");
                    }
                    FunctionEx<Path, Stream<T>> mapFn = readFileFnProvider.createReadFileFn(format);


                    return ReadFilesP.processor(dirAndGlob.f0(), dirAndGlob.f1(), sharedFileSystem, mapFn);
                }));
    }

    private static Tuple2<String, String> deriveDirectoryAndGlobFromPath(String path) {
        Path p = Paths.get(path).toAbsolutePath();

        String directory;
        String glob = "*";
        if (isDirectory(path)) {
            // The path is the directory and glob is used
            directory = p.toString();
        } else {
            Path parent = p.getParent();
            if (parent != null) {
                directory = parent.toString();

                Path fileName = p.getFileName();
                if (fileName != null) {
                    glob = fileName.toString();
                } else {
                    glob = "*";
                }
            } else {
                // p is root of the filesystem, has no parent
                directory = p.toString();
            }
        }
        return tuple2(directory, glob);
    }

    private static boolean isDirectory(String path) {
        return new File(path).isDirectory();
    }

    @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
    private abstract static class AbstractReadFileFnProvider implements ReadFileFnProvider {

        @Nonnull @Override
        public <T> FunctionEx<Path, Stream<T>> createReadFileFn(@Nonnull FileFormat<T> format) {
            FunctionEx<InputStream, Stream<T>> mapInputStreamFn = mapInputStreamFn(format);
            return path -> {
                FileInputStream fis = new FileInputStream(path.toFile());
                return mapInputStreamFn.apply(fis).onClose(() -> uncheckRun(fis::close));
            };
        }

        @Nonnull
        abstract <T> FunctionEx<InputStream, Stream<T>> mapInputStreamFn(FileFormat<T> format);
    }

    private static class JsonReadFileFnProvider extends AbstractReadFileFnProvider {

        @Nonnull
        @Override
        <T> FunctionEx<InputStream, Stream<T>> mapInputStreamFn(FileFormat<T> format) {
            JsonFileFormat<T> jsonFileFormat = (JsonFileFormat<T>) format;
            Class<T> thisClazz = jsonFileFormat.clazz();
            return is -> {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, UTF_8));

                return reader.lines()
                             .map(line -> uncheckCall(() -> JsonUtil.beanFrom(line, thisClazz)))
                             .onClose(() -> uncheckRun(reader::close));
            };
        }

        @Nonnull @Override
        public String format() {
            return JsonFileFormat.FORMAT_JSONL;
        }
    }

    private static class LinesReadFileFnProvider extends AbstractReadFileFnProvider {

        @Nonnull @Override
        @SuppressWarnings("unchecked")
        <T> FunctionEx<InputStream, Stream<T>> mapInputStreamFn(FileFormat<T> format) {
            LinesTextFileFormat linesTextFileFormat = (LinesTextFileFormat) format;
            String thisCharset = linesTextFileFormat.charset().name();
            return is -> {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, thisCharset));
                return (Stream<T>) reader.lines().onClose(() -> uncheckRun(reader::close));
            };
        }

        @Nonnull @Override
        public String format() {
            return LinesTextFileFormat.FORMAT_LINES;
        }
    }

    private static class ParquetReadFileFnProvider implements ReadFileFnProvider {

        @Nonnull @Override
        public <T> FunctionEx<Path, Stream<T>> createReadFileFn(@Nonnull FileFormat<T> format) {
            throw new UnsupportedOperationException("Reading Parquet files is not supported in local filesystem mode." +
                    " " +
                    "Use Jet Hadoop module with FileSourceBuilder.useHadoopForLocalFiles option instead.");
        }

        @Nonnull @Override
        public String format() {
            return ParquetFileFormat.FORMAT_PARQUET;
        }
    }

    private static class RawBytesReadFileFnProvider extends AbstractReadFileFnProvider {

        @Nonnull @Override
        @SuppressWarnings("unchecked")
        <T> FunctionEx<InputStream, Stream<T>> mapInputStreamFn(FileFormat<T> format) {
            return is -> (Stream<T>) Stream.of(IOUtil.readFully(is));
        }

        @Nonnull @Override
        public String format() {
            return RawBytesFileFormat.FORMAT_BIN;
        }
    }

    private static class TextReadFileFnProvider extends AbstractReadFileFnProvider {

        @Nonnull @Override
        @SuppressWarnings("unchecked")
        <T> FunctionEx<InputStream, Stream<T>> mapInputStreamFn(FileFormat<T> format) {
            TextFileFormat textFileFormat = (TextFileFormat) format;
            String thisCharset = textFileFormat.charset().name();
            return is -> (Stream<T>) Stream.of(new String(IOUtil.readFully(is), Charset.forName(thisCharset)));
        }

        @Nonnull @Override
        public String format() {
            return TextFileFormat.FORMAT_TXT;
        }
    }
}
