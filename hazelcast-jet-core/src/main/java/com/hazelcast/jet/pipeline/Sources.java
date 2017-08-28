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

package com.hazelcast.jet.pipeline;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.impl.connector.ReadIListP;
import com.hazelcast.jet.impl.connector.ReadWithPartitionIteratorP;
import com.hazelcast.jet.impl.connector.StreamFilesP;
import com.hazelcast.jet.impl.connector.StreamSocketP;
import com.hazelcast.jet.pipeline.impl.SinkImpl;
import com.hazelcast.jet.pipeline.impl.SourceImpl;
import com.hazelcast.jet.processor.SourceProcessors;

import javax.annotation.Nonnull;
import java.nio.charset.Charset;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class Sources {

    private static final String GLOB_WILDCARD = "*";

    private Sources() {
    }

    /**
     * Returns a source constructed directly from the given Core API processor
     * meta-supplier.
     *
     * @param sourceName user-friendly source name
     * @param metaSupplier the processor meta-supplier
     */
    public static <T> Source<T> fromProcessor(String sourceName, ProcessorMetaSupplier metaSupplier) {
        return new SourceImpl<>(sourceName, metaSupplier);
    }

    /**
     * Returns a source that will fetch entries from the Hazelcast {@code IMap}
     * with the specified name and will emit them as {@code Map.Entry}. Its
     * processors will only access data local to the member.
     * <p>
     * If the underlying map is concurrently being modified, there are no guarantees
     * given with respect to missing or duplicate items.
     */
    public static <K, V> Source<Map.Entry<K, V>> readMap(String mapName) {
        return new SourceImpl<>("readMap(" + mapName + ')', SourceProcessors.readMap(mapName));
    }

    /**
     * Returns a meta-supplier of processor that will fetch entries from a
     * Hazelcast {@code IMap} in a remote cluster. Processor will emit the
     * entries as {@code Map.Entry}.
     * <p>
     * If the underlying map is concurrently modified, there may be missing or
     * duplicate items.
     */
    @Nonnull
    public static <K, V> Source<Map.Entry<K, V>> readMap(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return new SourceImpl<>("readMap(" + mapName + ')', SourceProcessors.readMap(mapName, clientConfig));
    }

    /**
     * Returns a meta-supplier of processor that will fetch entries from the
     * Hazelcast {@code ICache} with the specified name and will emit them as
     * {@code Cache.Entry}. The processors will only access data local to the
     * member and, if {@code localParallelism} for the vertex is above one,
     * processors will divide the labor within the member so that each one gets
     * a subset of all local partitions to read.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will
     * have no partitions assigned to them.
     * <p>
     * If the underlying cache is concurrently modified, there may be missing
     * or duplicate items.
     */
    @Nonnull
    public static <K, V> Source<Map.Entry<K, V>> readCache(@Nonnull String cacheName) {
        return new SourceImpl<>("readCache(" + cacheName + ')', SourceProcessors.readCache(cacheName));
    }

    /**
     * Returns a meta-supplier of processor that will fetch entries from a
     * Hazelcast {@code ICache} in a remote cluster. Processor will emit the
     * entries as {@code Cache.Entry}.
     * <p>
     * If the underlying cache is concurrently modified, there may be missing
     * or duplicate items.
     */
    @Nonnull
    public static <K, V> Source<Map.Entry<K, V>> readCache(
            @Nonnull String cacheName, @Nonnull ClientConfig clientConfig
    ) {
        return new SourceImpl<>("readCache(" + cacheName + ')',
                ReadWithPartitionIteratorP.readCache(cacheName, clientConfig)
        );
    }

    /**
     * Returns a meta-supplier of processor that emits items retrieved from an
     * IMDG IList. Note that all elements from the list are emitted on a single
     * member &mdash; the one where the entire list is stored by the IMDG.
     */
    @Nonnull
    public static <E> Source<E> readList(@Nonnull String listName) {
        return new SourceImpl<>("readList(" + listName + ')', ReadIListP.supplier(listName));
    }

    /**
     * Returns a meta-supplier of processor that emits items retrieved from an
     * IMDG IList in a remote cluster. Note that all elements from the list are
     * emitted on a single member &mdash; the one where the entire list is
     * stored by the IMDG.
     */
    @Nonnull
    public static <E> Source<E> readList(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return new SourceImpl<>("readList(" + listName + ')', ReadIListP.supplier(listName, clientConfig));
    }

    /**
     * Returns a supplier of processor which connects to a specified socket and
     * reads and emits text line by line. This processor expects a server-side
     * socket to be available to connect to.
     * <p>
     * Each processor instance will create a socket connection to the configured
     * [host:port], so there will be {@code clusterSize * localParallelism}
     * connections. The server should do the load-balancing.
     * <p>
     * The processor will complete when the socket is closed by the server.
     * No reconnection is attempted.
     *
     * @param host The host name to connect to
     * @param port The port number to connect to
     * @param charset Character set used to decode the stream
     */
    @Nonnull
    public static Source<String> streamSocket(
            @Nonnull String host, int port, @Nonnull Charset charset
    ) {
        return new SourceImpl<>("streamSocket(" + host + ':' + port + ')',
                StreamSocketP.supplier(host, port, charset.name()));
    }

    /**
     * A source that emits lines from files in a directory (but not its
     * subdirectories. The files must not change while being read; if they do,
     * the behavior is unspecified.
     * <p>
     * To be useful, the source should be configured to read data local to
     * each member. For example, if the pathname resolves to a shared network
     * filesystem, it will emit duplicate data.
     *
     * @param directory parent directory of the files
     * @param charset charset to use to decode the files
     * @param glob the globbing mask, see {@link
     *             java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     *             Use {@code "*"} for all (non-special) files.
     */
    @Nonnull
    public static Source<String> readFiles(
            @Nonnull String directory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return new SourceImpl<>("readFiles(" + directory + '/' + glob + ')',
                ReadFilesP.supplier(directory, charset.name(), glob)
        );
    }

    /**
     * Convenience for {@link #readFiles(String, Charset, String) readFiles(directory, UTF_8, "*")}.
     */
    public static Source<String> readFiles(String directory) {
        return readFiles(directory, UTF_8, GLOB_WILDCARD);
    }

    /**
     * A source that generates a stream of lines of text coming from files in
     * the watched directory (but not its subdirectories). It will emit only
     * new contents added after startup: both new files and new content
     * appended to existing ones.
     * <p>
     * If the source observes a file that doesn't end with a newline, it will
     * ignore all the appended contents up to the next newline, so it only
     * emits full lines.
     * <p>
     * To be useful, the source should be configured to read data local to
     * each member. For example, if the pathname resolves to a shared network
     * filesystem, it will emit duplicate data.
     * <p>
     * The source completes when the directory is deleted. However, in order
     * to delete the directory, all files in it must be deleted and if you
     * delete a file that is currently being read from, the job may encounter
     * an {@code IOException}. The directory must be deleted on all nodes.
     * <p>
     * Any {@code IOException} will cause the job to fail.
     *
     * <h3>Limitation on Windows</h3>
     * On Windows the {@code WatchService} is not notified of appended lines
     * until the file is closed. If the file-writing process keeps the file
     * open while appending, the processor may fail to observe the changes.
     * It will be notified if any process tries to open that file, such as
     * looking at the file in Explorer. This holds for Windows 10 with the NTFS
     * file system and might change in future. You are advised to do your own
     * testing on your target Windows platform.
     *
     * <h3>Use the latest JRE</h3>
     * The underlying JDK API ({@link java.nio.file.WatchService}) has a
     * history of unreliability and this source may experience infinite
     * blocking, missed, or duplicate events as a result. Such problems may be
     * resolved by upgrading the JRE to the latest version.
     *
     * @param watchedDirectory pathname to the source directory
     * @param charset charset to use to decode the files
     * @param glob the globbing mask, see {@link
     *             java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     *             Use {@code "*"} for all (non-special) files.
     */
    public static Source<String> streamFiles(
            @Nonnull String watchedDirectory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return new SourceImpl<>("streamFiles(" + watchedDirectory + '/' + glob + ')',
                StreamFilesP.supplier(watchedDirectory, charset.name(), glob)
        );
    }

    /**
     * Convenience for {@link #streamFiles(String, Charset, String)
     * streamFiles(watchedDirectory, UTF_8, ".")}.
     */
    public static Source<String> streamFiles(@Nonnull String watchedDirectory) {
        return streamFiles(watchedDirectory, UTF_8, GLOB_WILDCARD);
    }
}
