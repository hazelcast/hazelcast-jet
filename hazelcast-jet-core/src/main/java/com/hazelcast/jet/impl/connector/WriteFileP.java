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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionId;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;
import com.hazelcast.jet.impl.processor.UnboundedTransactionsProcessorUtility;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * A file-writing sink supporting rolling files. Rolling can occur on:
 * - at a snapshot boundary
 * - when a file size is reached
 * - when a date changes
 */
public final class WriteFileP<T> implements Processor {

    static final String TEMP_FILE_SUFFIX = ".tmp";
    private static final LongSupplier SYSTEM_CLOCK = (LongSupplier & Serializable) System::currentTimeMillis;

    private final Path directory;
    private final FunctionEx<? super T, ? extends String> toStringFn;
    private final Charset charset;
    private final boolean append;
    private final DateTimeFormatter dateFormatter;
    private final Long maxFileSize;
    private final LongSupplier clock;

    private UnboundedTransactionsProcessorUtility<FileId, FileResource> utility;
    private Context context;
    private int fileSequence;
    private SizeTrackingStream sizeTrackingStream;
    private String lastFileDate;

    /**
     * Rolling by date is based on system clock, not on event time.
     */
    private WriteFileP(
            @Nonnull String directoryName,
            @Nonnull FunctionEx<? super T, ? extends String> toStringFn,
            @Nonnull String charset,
            boolean append,
            @Nullable String dateFormatter,
            @Nullable Long maxFileSize,
            @Nonnull LongSupplier clock
    ) {
        this.directory = Paths.get(directoryName);
        this.toStringFn = toStringFn;
        this.charset = Charset.forName(charset);
        this.append = append;
        this.dateFormatter = dateFormatter != null ? DateTimeFormatter.ofPattern(dateFormatter) : null;
        this.maxFileSize = maxFileSize;
        this.clock = clock;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) throws IOException {
        this.context = context;
        Files.createDirectories(directory);

        utility = new UnboundedTransactionsProcessorUtility<>(
                outbox,
                context,
                context.processingGuarantee(),
                this::newFileName,
                this::newFileResource,
                this::recoverAndCommit,
                this::abortUnfinishedTransactions
        );
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        // roll file on date change
        if (dateFormatter != null && !dateFormatter.format(Instant.ofEpochMilli(clock.getAsLong())).equals(lastFileDate)) {
            fileSequence = 0;
            utility.newActiveTransaction();
        }
        FileResource transaction = utility.activeTransaction();
        Writer writer = transaction.writer();
        try {
            for (Object item; (item = inbox.poll()) != null; ) {
                @SuppressWarnings("unchecked")
                    T castedItem = (T) item;
                writer.write(toStringFn.apply(castedItem));
                writer.write(System.lineSeparator());
                if (maxFileSize != null && sizeTrackingStream.size >= maxFileSize) {
                    writer = utility.newActiveTransaction().writer();
                }
            }
            writer.flush();
            if (maxFileSize != null && sizeTrackingStream.size >= maxFileSize) {
                utility.newActiveTransaction();
            }
        } catch (IOException e) {
            throw new RestartableException(e);
        }
    }

    @Override
    public boolean complete() {
        utility.afterCompleted();
        return true;
    }

    @Override
    public void close() throws Exception {
        if (utility != null) {
            utility.close();
        }
    }

    @Override
    public boolean saveToSnapshot() {
        return utility.saveToSnapshot();
    }

    @Override
    public boolean onSnapshotCompleted(boolean commitTransactions) {
        return utility.onSnapshotCompleted(commitTransactions);
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Inbox inbox) {
        utility.restoreFromSnapshot(inbox);
    }

    @Override
    public boolean finishSnapshotRestore() {
        return utility.finishSnapshotRestore();
    }

    private FileId newFileName() {
        StringBuilder sb = new StringBuilder();
        if (dateFormatter != null) {
            lastFileDate = dateFormatter.format(Instant.ofEpochMilli(clock.getAsLong()));
            sb.append(lastFileDate);
            sb.append('-');
        }
        sb.append(context.globalProcessorIndex());
        String file = sb.toString();
        boolean usesSequence = utility.externalGuarantee() == EXACTLY_ONCE || maxFileSize != null;
        if (usesSequence) {
            // TODO [viliam] always check existing files, if sequence is used. We might only
            //  build the set only if the first sequence fails and only if fileSequence == 0, otherwise
            //  we can just check the generated file
            Set<String> existingFiles;
            if (utility.externalGuarantee() == EXACTLY_ONCE) {
                try (Stream<Path> fileListStream = Files.list(directory)) {
                    existingFiles = fileListStream
                            .map(f -> f.getFileName().toString())
                            .collect(Collectors.toSet());
                } catch (IOException e) {
                    throw sneakyThrow(e);
                }
            } else {
                existingFiles = Collections.emptySet();
            }
            int prefixLength = sb.length();
            do {
                sb.append('-').append(fileSequence++);
                file = sb.toString();
                sb.setLength(prefixLength);
            } while (existingFiles.contains(file) || existingFiles.contains(file + TEMP_FILE_SUFFIX));
        }
        return new FileId(file, context.globalProcessorIndex());
    }

    /**
     * Returns a FileResource for a fileId.
     */
    private FileResource newFileResource(FileId fileId) {
        logFine(context.logger(), "Creating %s", fileId.fileName); // TODO [viliam] logFinest
        return utility.externalGuarantee() == EXACTLY_ONCE
                ? new FileWithTransaction(fileId)
                : new FileWithoutTransaction(fileId);
    }

    private void recoverAndCommit(FileId fileId) throws IOException {
        this.context.logger().info("aaa recoverAndCommit " + fileId);
        Path tempFile = directory.resolve(fileId.fileName + TEMP_FILE_SUFFIX);
        if (Files.exists(tempFile)) {
            Files.move(tempFile, directory.resolve(fileId.fileName), StandardCopyOption.ATOMIC_MOVE);
        } else {
            context.logger().warning("Uncommitted temporary file from previous execution didn't exist, data loss " +
                    "might occur: " + tempFile);
        }
    }

    private void abortUnfinishedTransactions() {
        try (Stream<Path> fileStream = Files.list(directory)) {
            fileStream
                    .filter(file -> file.getFileName().toString().endsWith(TEMP_FILE_SUFFIX))
                    .forEach(file -> uncheckRun(() -> {
                        context.logger().fine("Deleting " + file);
                        Files.delete(file);
                    }));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Writer createWriter(Path file) {
        try {
            context.logger().info("aaa creating " + file);
            FileOutputStream fos = new FileOutputStream(file.toFile(), append);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            sizeTrackingStream = new SizeTrackingStream(bos);
            return new OutputStreamWriter(sizeTrackingStream, charset);
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    /**
     * Use method in {@link SinkProcessors} instead. TODO [viliam]
     */
    public static <T> ProcessorMetaSupplier metaSupplier(
            @Nonnull String directoryName,
            @Nonnull FunctionEx<? super T, ? extends String> toStringFn,
            @Nonnull String charset,
            boolean append,
            @Nullable String datePattern,
            @Nullable Long maxFileSize
    ) {
        return metaSupplier(directoryName, toStringFn, charset, append, datePattern, maxFileSize, SYSTEM_CLOCK);
    }

    // for tests
    static <T> ProcessorMetaSupplier metaSupplier(
            @Nonnull String directoryName,
            @Nonnull FunctionEx<? super T, ? extends String> toStringFn,
            @Nonnull String charset,
            boolean append,
            @Nullable String datePattern,
            @Nullable Long maxFileSize,
            @Nonnull LongSupplier clock
    ) {
        return ProcessorMetaSupplier.preferLocalParallelismOne(() -> new WriteFileP<>(directoryName, toStringFn,
                charset, append, datePattern, maxFileSize, clock));
    }

    private abstract class FileResource implements TransactionalResource<FileId> {

        final FileId fileId;
        final Path targetFile;
        Writer writer;

        @SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
                justification = "targetFile always has fileName")
        FileResource(FileId fileId) {
            this.fileId = fileId;
            this.targetFile = directory.resolve(fileId.fileName);
        }

        @Override
        public FileId id() {
            return fileId;
        }

        @Override
        public void beginTransaction() {
        }

        @Override
        public void prepare() throws IOException {
            context.logger().info("aaa prepare " + id());
            release();
        }

        @Override
        public void release() throws IOException {
            if (writer != null) {
                context.logger().info("aaa release (maybe prepare?) " + id());
                writer.close();
                writer = null;
            }
        }

        public Writer writer() {
            return writer;
        }
    }

    private final class FileWithoutTransaction extends FileResource {

        FileWithoutTransaction(@Nonnull FileId fileId) {
            super(fileId);
        }

        @Override
        public Writer writer() {
            if (writer == null) {
                writer = createWriter(targetFile);
            }
            return super.writer();
        }
    }

    private final class FileWithTransaction extends FileResource {

        private final Path tempFile;

        FileWithTransaction(@Nonnull FileId fileId) {
            super(fileId);
            tempFile = directory.resolve(fileId + TEMP_FILE_SUFFIX);
        }

        @Override
        public void beginTransaction() {
            writer = createWriter(tempFile);
        }

        @Override
        public void commit() throws IOException {
            context.logger().info("aaa committing " + tempFile);
            if (writer != null) {
                writer.close();
                writer = null;
            }
            Files.move(tempFile, targetFile, StandardCopyOption.ATOMIC_MOVE);
        }
    }

    private static final class SizeTrackingStream extends OutputStream {
        private final OutputStream target;
        private long size;

        private SizeTrackingStream(OutputStream target) {
            this.target = target;
        }

        @Override
        public void write(int b) throws IOException {
            size++;
            target.write(b);
        }

        @Override
        public void write(@Nonnull byte[] b, int off, int len) throws IOException {
            size += len;
            target.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
            target.close();
        }

        @Override
        public void flush() throws IOException {
            target.flush();
        }
    }

    // TODO [viliam] better serialization
    private static final class FileId implements TransactionId, Serializable {
        private final String fileName;
        private final int index;

        private FileId(String fileName, int index) {
            this.fileName = fileName;
            this.index = index;
        }

        @Override
        public int index() {
            return index;
        }

        @Override
        public String toString() {
            return "FileId{" + fileName + '}';
        }
    }
}
