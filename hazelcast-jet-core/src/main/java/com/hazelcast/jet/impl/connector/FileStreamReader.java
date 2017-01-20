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
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ExceptionUtil;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.sun.nio.file.SensitivityWatchEventModifier.HIGH;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

/**
 * A producer reads files from directory and emits them lines by lines to the next processor
 */
public class FileStreamReader extends AbstractProcessor {

    private static final ILogger LOGGER = com.hazelcast.logging.Logger.getLogger(FileStreamReader.class);
    private final String filePath;
    private final WatchType watchType;
    private final int parallelism;
    private final int id;
    private WatchService watcher;
    private Path path;

    public enum WatchType {

        /**
         * Process only new files.
         */
        NEW,

        /**
         * Re-process files from beginning when a modification occurs on them.
         */
        REPROCESS,

        /**
         * Process only appended content to the files.
         */
        APPENDED_ONLY
    }

    private Map<String, Long> fileOffsets = new HashMap<>();


    protected FileStreamReader(String filePath, WatchType watchType, int parallelism, int id) {
        this.filePath = filePath;
        this.watchType = watchType;
        this.parallelism = parallelism;
        this.id = id;
    }

    @Override
    protected void init(@Nonnull Context context) {
        try {
            watcher = FileSystems.getDefault().newWatchService();
            path = Paths.get(filePath);
            path.register(watcher, new WatchEvent.Kind[]{ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE}, HIGH);
            LOGGER.info("Started to watch the directory : " + filePath);
        } catch (IOException e) {
            LOGGER.severe("Error occurred while watching directories, error : " + e.getMessage());
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean complete() {
        try {
            while (true) {
                final WatchKey key;
                try {
                    key = watcher.take();
                } catch (InterruptedException x) {
                    return true;
                }

                for (WatchEvent<?> event : key.pollEvents()) {
                    final WatchEvent.Kind<?> kind = event.kind();
                    final WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    final Path file = ev.context();
                    final Path resolved = path.resolve(file);
                    if (kind == OVERFLOW) {
                        continue;
                    }
                    if (kind == ENTRY_DELETE) {
                        LOGGER.info("Directory (" + filePath + ") deleted, stopped watching");
                        watcher.close();
                        return true;
                    }

                    final int filenameHash = resolved.toFile().getPath().hashCode();
                    if (((filenameHash & Integer.MAX_VALUE) % parallelism) != id) {
                        continue;
                    }

                    if (kind == ENTRY_CREATE) {
                        LOGGER.info("Detected new file: " + resolved);
                        readFile(resolved, 0L);
                        return !key.reset();
                    }
                    if (kind != ENTRY_MODIFY) {
                        continue;
                    }
                    LOGGER.info("Detected file change: " + resolved);
                    switch (watchType) {
                        case REPROCESS:
                            LOGGER.info("Re-processing the file (" + resolved + ')');
                            fileOffsets.put(resolved.toString(), 0L);
                            readFile(resolved, 0L);
                            return !key.reset();
                        case APPENDED_ONLY:
                            LOGGER.info("Processing only the appended content on the file (" + resolved + ')');
                            readFile(resolved, fileOffsets.computeIfAbsent(resolved.toString(), s -> 0L));
                            return !key.reset();
                        case NEW:
                        default:
                    }
                }
                boolean valid = key.reset();
                if (!valid) {
                    break;
                }
            }
        } catch (IOException e) {
            LOGGER.severe("Error occurred while watching directories, error : " + e.getMessage());
            return true;
        }
        return true;
    }

    private void readFile(Path file, long offset) throws IOException {
        FileInputStream fis = new FileInputStream(file.toFile());
        fis.getChannel().position(offset);
        String line;
        long position;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"))) {
            while ((line = reader.readLine()) != null) {
                LOGGER.finest("line = " + line);
                emit(line);
            }
            position = fis.getChannel().position();
        }
        fileOffsets.put(file.toString(), position);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    /**
     * Creates a supplier for {@link FileStreamReader}
     *
     * @param filePath
     * @param watchType
     */
    public static ProcessorSupplier supplier(String filePath, WatchType watchType) {
        return new Supplier(filePath, watchType);
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;
        private final String filePath;
        private final WatchType watchType;

        Supplier(String filePath, WatchType watchType) {
            this.filePath = filePath;
            this.watchType = watchType;
        }

        @Nonnull
        @Override
        public List<Processor> get(int count) {
            List<Processor> processors = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                processors.add(new FileStreamReader(filePath, watchType, count, i));
            }
            return processors;
        }
    }

}
