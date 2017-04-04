/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * A source processor designed to process files in a directory in a batch. It
 * processes all files in a directory (optionally filtering with a {@code glob},
 * see {@link java.nio.file.FileSystem#getPathMatcher(String)}). Contents of the
 * files are emitted line by line. There is no indication, which file a particular
 * line comes from. Contents of subdirectories are not processed.
 * <p>
 * The same directory must be available on all members, but it should not
 * contain the same files (i.e. it should not be a network shared directory, but
 * files local to the machine).
 * <p>
 * If directory contents are changed while processing, the behavior is
 * undefined: the changed contents might or might not be processed.
 */
public class ReadFileP extends AbstractProcessor {

    private final Charset charset;
    private final int parallelism;
    private final int id;
    private final Path directory;
    private final String glob;

    ReadFileP(String directory, Charset charset, String glob, int parallelism, int id) {
        this.directory = Paths.get(directory);
        this.glob = glob;
        this.charset = charset;
        this.parallelism = parallelism;
        this.id = id;
    }

    @Override
    public boolean complete() {
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory, glob == null ? "*" : glob)) {
            StreamSupport.stream(directoryStream.spliterator(), false)
                    .filter(this::shouldProcessEvent)
                    .forEach(this::processFile);
        } catch (IOException e) {
            throw sneakyThrow(e);
        }

        return true;
    }

    private boolean shouldProcessEvent(Path file) {
        int hashCode = file.hashCode();
        return ((hashCode & Integer.MAX_VALUE) % parallelism) == id;
    }

    private void processFile(Path file) {
        if (getLogger().isFinestEnabled()) {
            getLogger().finest("Processing file " + file);
        }

        try (BufferedReader reader = Files.newBufferedReader(file, charset)) {
            for (String line; (line = reader.readLine()) != null; ) {
                emit(line);
            }
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    /**
     * Creates a supplier for {@link ReadFileP}
     *
     * @param directory the folder to process files from
     */
    public static ProcessorSupplier supplier(String directory, String charset, String glob) {
        return new Supplier(directory, charset, glob);
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;
        private final String watchedDirectory;
        private final String charset;
        private final String glob;

        private transient ArrayList<ReadFileP> readers;

        Supplier(String watchedDirectory, String charset, String glob) {
            this.watchedDirectory = watchedDirectory;
            this.charset = charset;
            this.glob = glob;
        }

        @Override @Nonnull
        public List<ReadFileP> get(int count) {
            readers = new ArrayList<>(count);
            Charset charsetObj = charset == null ? StandardCharsets.UTF_8 : Charset.forName(charset);
            for (int i = 0; i < count; i++) {
                readers.add(new ReadFileP(watchedDirectory, charsetObj, glob, count, i));
            }
            return readers;
        }
    }
}
