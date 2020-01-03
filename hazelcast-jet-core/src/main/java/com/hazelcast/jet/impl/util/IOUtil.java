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

package com.hazelcast.jet.impl.util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static java.util.Collections.singletonList;

public final class IOUtil {

    private static final int BUFFER_SIZE = 1 << 14;

    private IOUtil() {
    }

    /**
     * Creates a ZIP-file stream from the directory tree rooted at the supplied
     * {@code baseDir}. Pipes the stream into the provided output stream, closing
     * it when done.
     * <p>
     * <strong>Note:</strong> hidden files and directories are ignored
     */
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
            justification = "it's a false positive since java 11: https://github.com/spotbugs/spotbugs/issues/756")
    public static void copyDirectoryAsZip(@Nonnull Path baseDir, @Nonnull OutputStream destination)
            throws IOException {
        try (ZipOutputStream zipOut = new ZipOutputStream(destination)) {
            Queue<Path> dirQueue = new ArrayDeque<>(singletonList(baseDir));
            for (Path dirPath; (dirPath = dirQueue.poll()) != null; ) {
                if (Files.isHidden(dirPath)) {
                    continue;
                }
                try (DirectoryStream<Path> listing = Files.newDirectoryStream(dirPath)) {
                    Iterator<Path> iter = listing.iterator();
                    if (!iter.hasNext()) {
                        // Write this empty directory as an explicit ZIP entry
                        zipOut.putNextEntry(new ZipEntry(baseDir.relativize(dirPath).toString()));
                        zipOut.closeEntry();
                        continue;
                    }
                    // DirectoryStream.iterator() may not be called twice
                    for (Path filePath : (Iterable<Path>) () -> iter) {
                        if (Files.isDirectory(filePath)) {
                            dirQueue.add(filePath);
                            continue;
                        }
                        if (Files.isHidden(filePath)) {
                            continue;
                        }
                        zipOut.putNextEntry(new ZipEntry(baseDir.relativize(filePath).toString()));
                        try (InputStream in = Files.newInputStream(filePath)) {
                            copyStream(in, zipOut);
                        }
                        zipOut.closeEntry();
                    }
                }
            }
        }
    }

    /**
     * Creates a ZIP-file stream from the supplied file. The file will be the
     * sole entry in the created zip.
     */
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
            justification = "it's a false positive since java 11: https://github.com/spotbugs/spotbugs/issues/756")
    public static void copyFileAsZip(@Nonnull URL url, @Nonnull OutputStream destination)
            throws IOException, URISyntaxException {
        try (ZipOutputStream zipOut = new ZipOutputStream(destination)) {
            Path fileName = Paths.get(url.toURI()).getFileName();
            if (fileName == null) {
                throw new IllegalArgumentException("filePath is empty");
            }
            zipOut.putNextEntry(new ZipEntry(fileName.toString()));
            try (InputStream in = url.openStream()) {
                copyStream(in, zipOut);
            }
        }
    }

    public static void copyStream(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[BUFFER_SIZE];
        for (int readCount; (readCount = in.read(buf)) > 0; ) {
            out.write(buf, 0, readCount);
        }
    }

    @Nonnull
    public static byte[] readFully(@Nonnull InputStream in) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] b = new byte[BUFFER_SIZE];
            for (int len; (len = in.read(b)) != -1; ) {
                out.write(b, 0, len);
            }
            return out.toByteArray();
        }
    }

    public static void unzip(InputStream is, Path targetPath) throws IOException {
        try (ZipInputStream zipIn = new ZipInputStream(is)) {
            for (ZipEntry ze; (ze = zipIn.getNextEntry()) != null; ) {
                String entryName = ze.getName();
                Path destPath = targetPath.resolve(entryName);
                if (ze.isDirectory()) {
                    Files.createDirectory(destPath);
                } else {
                    Path parent = destPath.getParent();
                    if (parent != null) {
                        Files.createDirectories(parent);
                        try (OutputStream fileOut = Files.newOutputStream(destPath)) {
                            copyStream(zipIn, fileOut);
                        }
                    }
                }
            }
        }
    }
}
