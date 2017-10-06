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

package com.hazelcast.jet;

import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.SourceImpl;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.impl.connector.ReadIListP;
import com.hazelcast.jet.impl.connector.ReadWithPartitionIteratorP;
import com.hazelcast.jet.impl.connector.StreamFilesP;
import com.hazelcast.jet.impl.connector.StreamSocketP;
import com.hazelcast.map.journal.EventJournalMapEvent;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Contains factory methods for various types of pipeline sources. To start
 * building a pipeline, pass a source to {@link Pipeline#drawFrom(Source)}
 * and you will obtain the initial {@link ComputeStage}. You can then
 * attach further stages to it.
 * <p>
 * The same pipeline may contain more than one source, each starting its
 * own branch. The branches may be merged with multiple-input transforms
 * such as co-group and hash-join.
 */
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
    public static <T> Source<T> fromProcessor(@Nonnull String sourceName, @Nonnull ProcessorMetaSupplier metaSupplier) {
        return new SourceImpl<>(sourceName, metaSupplier);
    }

    /**
     * Returns a source constructed directly from the given Core API processor
     * supplier.
     *
     * @param sourceName user-friendly source name
     * @param supplier the processor supplier
     */
    public static <T> Source<T> fromProcessor(@Nonnull String sourceName, @Nonnull ProcessorSupplier supplier) {
        return new SourceImpl<>(sourceName, ProcessorMetaSupplier.of(supplier));
    }

    /**
     * Returns a source constructed directly from the given supplier of Core API processors.
     *
     * @param sourceName user-friendly source name
     * @param supplier the supplier of processors
     */
    public static <T> Source<T> fromProcessor(
            @Nonnull String sourceName, @Nonnull DistributedSupplier<Processor> supplier) {
        return new SourceImpl<>(sourceName, ProcessorMetaSupplier.of(supplier));
    }

    /**
     * Returns a source that fetches entries from the Hazelcast {@code IMap}
     * with the specified name and emits them as {@code Map.Entry}. Its
     * processors will leverage data locality by fetching only those entries
     * that are stored on the member where they are running.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will
     * have no partitions assigned to them.
     * <p>
     * This source does not save any state to snapshot. If the job is restarted,
     * all entries will be emitted again.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     */
    public static <K, V> Source<Map.Entry<K, V>> readMap(@Nonnull String mapName) {
        return fromProcessor("readMap(" + mapName + ')', SourceProcessors.readMapP(mapName));
    }

    /**
     * Returns a source that fetches entries from the Hazelcast {@code IMap}
     * with the specified name, filters them using the supplied predicate,
     * transforms them using the supplied projection function, and emits
     * them as {@code Map.Entry}. Its processors will leverage data locality
     * by fetching only those entries that are stored on the member where they
     * are running.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will
     * have no partitions assigned to them.
     * <p>
     * This source does not save any state to snapshot. If the job is restarted,
     * all entries will be emitted again.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     */
    public static <K, V, T> Source<T> readMap(@Nonnull String mapName,
                                              @Nonnull DistributedPredicate<Map.Entry<K, V>> predicate,
                                              @Nonnull DistributedFunction<Map.Entry<K, V>, T> projectionFn) {
        return fromProcessor("readMap(" + mapName + ')',
                SourceProcessors.readMapP(mapName, predicate, projectionFn));
    }

    /**
     * Convenience for {@link #streamMap(String, DistributedPredicate,
     * DistributedFunction, boolean)} with no projection or filtering. It
     * emits {@link EventJournalMapEvent}s.
     */
    @Nonnull
    public static <K, V> Source<EventJournalMapEvent<K, V>> streamMap(
            @Nonnull String mapName, boolean startFromLatestSequence) {
        return fromProcessor("streamMap(" + mapName + ')',
                SourceProcessors.streamMapP(mapName, startFromLatestSequence));
    }

    /**
     * Returns a source that will stream the {@link EventJournalMapEvent}
     * events of the Hazelcast {@code IMap} with the specified name. Given
     * predicate and projection will be applied to the events at the source.
     * <p>
     * The processors will only access data local to the member and, if {@code
     * localParallelism} for the vertex is above one, processors will divide
     * the labor within the member so that each of them will get a subset of
     * all local partitions to stream.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will have
     * no partitions assigned to them.
     * <p>
     * In order to stream from a map, event-journal should be configured.
     * See {@link com.hazelcast.config.EventJournalConfig}.
     * <p>
     * Journal offset is saved to the state snapshot. If the job restarts,
     * emission starts at the saved offset, giving exactly-once guarantee.
     *
     * @param mapName the name of the map
     * @param predicate the predicate to filter the events, can be {@code null}
     * @param projection the projection to map the events, can be {@code null}
     * @param startFromLatestSequence starting point of the events in event journal.
     *          {@code true} to start from latest, {@code false} to start from oldest
     * @param <T> type of emitted item
     */
    @Nonnull
    public static <K, V, T> Source<T> streamMap(@Nonnull String mapName,
                                          @Nullable DistributedPredicate<EventJournalMapEvent<K, V>> predicate,
                                          @Nullable DistributedFunction<EventJournalMapEvent<K, V>, T> projection,
                                          boolean startFromLatestSequence) {
        return fromProcessor("streamMap(" + mapName + ')',
                SourceProcessors.streamMapP(mapName, predicate, projection, startFromLatestSequence));
    }

    /**
     * Returns a source that fetches entries from the Hazelcast {@code IMap}
     * with the specified name in a remote cluster identified by the supplied
     * {@code ClientConfig} and emits them as {@code Map.Entry}.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will
     * have no partitions assigned to them.
     * <p>
     * This source does not save any state to snapshot. If the job is restarted,
     * all entries will be emitted again.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     */
    @Nonnull
    public static <K, V> Source<Map.Entry<K, V>> readMap(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return fromProcessor("readMap(" + mapName + ')', SourceProcessors.readMapP(mapName, clientConfig));
    }

    /**
     * Returns a source that fetches entries from the Hazelcast {@code IMap}
     * with the specified name in a remote cluster identified by the supplied
     * {@code ClientConfig}, filters them using the supplied predicate,
     * transforms them using the supplied projection function, and emits
     * them as {@code Map.Entry}.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will
     * have no partitions assigned to them.
     * <p>
     * This source does not save any state to snapshot. If the job is restarted,
     * all entries will be emitted again.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     */
    public static <K, V, T> Source<T> readMap(@Nonnull String mapName,
                                              @Nonnull DistributedPredicate<Map.Entry<K, V>> predicate,
                                              @Nonnull DistributedFunction<Map.Entry<K, V>, T> projectionFn,
                                              @Nonnull ClientConfig clientConfig) {
        return fromProcessor("readMap(" + mapName + ')',
                SourceProcessors.readMapP(mapName, predicate, projectionFn, clientConfig));
    }

    /**
     * Convenience for {@link #streamMap(String, ClientConfig,
     * DistributedPredicate, DistributedFunction, boolean)} with no projection
     * or filtering. It emits {@link EventJournalMapEvent}s.
     */
    @Nonnull
    public static <K, V> Source<EventJournalMapEvent<K, V>> streamMap(
            @Nonnull String mapName, @Nonnull ClientConfig clientConfig, boolean startFromLatestSequence) {
        return fromProcessor("streamMap(" + mapName + ')',
                SourceProcessors.streamMapP(mapName, clientConfig, startFromLatestSequence));
    }

    /**
     * Returns a source that will stream the {@link EventJournalMapEvent}
     * events of the Hazelcast {@code IMap} with the specified name from a
     * remote cluster. Given predicate and projection will be applied to the
     * events at the source.
     * <p>
     * In order to stream from a map, event-journal should be configured.
     * Please see {@link com.hazelcast.config.EventJournalConfig}
     * <p>
     * Journal offset is saved to the state snapshot. If the job restarts,
     * emission starts at the saved offset, giving exactly-once guarantee.
     *
     * @param mapName the name of the map
     * @param clientConfig configuration for the client to connect to the remote cluster
     * @param predicate the predicate to filter the events, can be {@code null}
     * @param projection the projection to map the events, can be {@code null}
     * @param startFromLatestSequence starting point of the events in event journal.
     *          {@code true} to start from latest, {@code false} to start from oldest.
     * @param <K> type of key
     * @param <V> type of value
     * @param <T> type of emitted item
     */
    @Nonnull
    public static <K, V, T> Source<T> streamMap(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nullable DistributedPredicate<EventJournalMapEvent<K, V>> predicate,
            @Nullable DistributedFunction<EventJournalMapEvent<K, V>, T> projection,
            boolean startFromLatestSequence
    ) {
        return fromProcessor("streamMap(" + mapName + ')',
                SourceProcessors.streamMapP(mapName, clientConfig, predicate, projection, startFromLatestSequence));
    }

    /**
     * Returns a source that fetches entries from the Hazelcast {@code ICache}
     * with the specified name and emits them as {@code Map.Entry}. Its
     * processors will leverage data locality by fetching only those entries
     * that are stored on the member where they are running.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will
     * have no partitions assigned to them.
     * <p>
     * This source does not save any state to snapshot. If the job is restarted,
     * all entries will be emitted again.
     * <p>
     * If the {@code ICache} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     */
    @Nonnull
    public static <K, V> Source<Map.Entry<K, V>> readCache(@Nonnull String cacheName) {
        return fromProcessor("readCache(" + cacheName + ')', SourceProcessors.readCacheP(cacheName));
    }

    /**
     * Convenience for {@link #streamCache(String, DistributedPredicate,
     * DistributedFunction, boolean)} with no projection or filtering.
     */
    @Nonnull
    public static <K, V> Source<EventJournalCacheEvent<K, V>> streamCache(
            @Nonnull String cacheName, boolean startFromLatestSequence) {
        return fromProcessor("streamCache(" + cacheName + ')',
                SourceProcessors.streamCacheP(cacheName, startFromLatestSequence));
    }

    /**
     * Returns a source that will stream the {@link EventJournalCacheEvent}
     * events of the Hazelcast {@code ICache} with the specified name. Given
     * predicate and projection will be applied to the events at the source.
     * <p>
     * The processors will only access data local to the member and, if {@code
     * localParallelism} for the vertex is above one, processors will divide
     * the labor within the member so that each one gets a subset of all local partitions to stream.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will have
     * no partitions assigned to them.
     * <p>
     * In order to stream from a cache, event-journal should be configured.
     * Please see {@link com.hazelcast.config.EventJournalConfig}.
     * <p>
     * Journal offset is saved to the state snapshot. If the job restarts,
     * emission starts at the saved offset, giving exactly-once guarantee.
     *
     * @param cacheName               The name of the cache
     * @param predicate               The predicate to filter the events, can be null
     * @param projection              The projection to map the events, can be null
     * @param startFromLatestSequence starting point of the events in event journal
     *                                {@code true} to start from latest, {@code false} to start from oldest
     * @param <T>                     type of emitted item
     */
    @Nonnull
    public static <K, V, T> Source<T> streamCache(
            @Nonnull String cacheName,
            @Nullable DistributedPredicate<EventJournalCacheEvent<K, V>> predicate,
            @Nullable DistributedFunction<EventJournalCacheEvent<K, V>, T> projection,
            boolean startFromLatestSequence
    ) {
        return fromProcessor("streamCache(" + cacheName + ')',
                SourceProcessors.streamCacheP(cacheName, predicate, projection, startFromLatestSequence));
    }

    /**
     * Returns a source that fetches entries from the Hazelcast {@code ICache}
     * with the specified name in a remote cluster identified by the supplied
     * {@code ClientConfig} and emits them as {@code Map.Entry}.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will
     * have no partitions assigned to them.
     * <p>
     * This source does not save any state to snapshot. If the job is restarted,
     * all entries will be emitted again.
     * <p>
     * If the {@code ICache} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     */
    @Nonnull
    public static <K, V> Source<Map.Entry<K, V>> readCache(
            @Nonnull String cacheName, @Nonnull ClientConfig clientConfig
    ) {
        return fromProcessor("readCache(" + cacheName + ')',
                ReadWithPartitionIteratorP.readCache(cacheName, clientConfig)
        );
    }

    /**
     * Convenience for {@link #streamCache(String, ClientConfig,
     * DistributedPredicate, DistributedFunction, boolean)} with no projection
     * or filtering. It emits {@link EventJournalCacheEvent}s.
     */
    @Nonnull
    public static <K, V> Source<EventJournalCacheEvent<K, V>> streamCache(
            @Nonnull String cacheName, @Nonnull ClientConfig clientConfig, boolean startFromLatestSequence
    ) {
        return fromProcessor("streamCache(" + cacheName + ')',
                SourceProcessors.streamCacheP(cacheName, clientConfig, startFromLatestSequence));
    }

    /**
     * Returns a source that will stream the {@link EventJournalCacheEvent}
     * events of the Hazelcast {@code ICache} with the specified name from a
     * remote cluster. Given predicate and projection will be applied to the
     * events at the source.
     * <p>
     * In order to stream from a cache, event-journal should be configured.
     * Please see {@link com.hazelcast.config.EventJournalConfig}.
     * <p>
     * Journal offset is saved to the state snapshot. If the job restarts,
     * emission starts at the saved offset, giving exactly-once guarantee.
     *
     * @param cacheName               The name of the cache
     * @param clientConfig            configuration for the client to connect to the remote cluster
     * @param predicate               The predicate to filter the events, can be null
     * @param projection              The projection to map the events, can be null
     * @param startFromLatestSequence starting point of the events in event journal
     *                                {@code true} to start from latest, {@code false} to start from oldest
     * @param <T>                     type of emitted item
     */
    @Nonnull
    public static <K, V, T> Source<T> streamCache(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            @Nullable DistributedPredicate<EventJournalCacheEvent<K, V>> predicate,
            @Nullable DistributedFunction<EventJournalCacheEvent<K, V>, T> projection,
            boolean startFromLatestSequence
    ) {
        return fromProcessor("streamCache(" + cacheName + ')',
                SourceProcessors.streamCacheP(cacheName, clientConfig, predicate, projection, startFromLatestSequence));
    }

    /**
     * Returns a source that emits items retrieved from a Hazelcast {@code
     * IList}. All elements are emitted on a single member &mdash; the one
     * where the entire list is stored by the IMDG.
     * <p>
     * This source does not save any state to snapshot. If the job is restarted,
     * all entries will be emitted again.
     */
    @Nonnull
    public static <E> Source<E> readList(@Nonnull String listName) {
        return fromProcessor("readList(" + listName + ')', ReadIListP.supplier(listName));
    }

    /**
     * Returns a source that emits items retrieved from a Hazelcast {@code
     * IList} in a remote cluster identified by the supplied {@code
     * ClientConfig}. All elements are emitted on a single member.
     * <p>
     * This source does not save any state to snapshot. If the job is restarted,
     * all entries will be emitted again.
     */
    @Nonnull
    public static <E> Source<E> readList(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return fromProcessor("readList(" + listName + ')', ReadIListP.supplier(listName, clientConfig));
    }

    /**
     * Returns a source which connects to the specified socket and emits lines
     * of text received from it. It decodes the text using the supplied {@code
     * charset}.
     * <p>
     * Each underlying processor opens its own TCP connection, so there will be
     * {@code clusterSize * localParallelism} open connections to the server.
     * <p>
     * The source completes when the server closes the socket. It never attempts
     * to reconnect. Any {@code IOException} will cause the job to fail.
     * <p>
     * This source does not save any state to snapshot. On job restart, it will
     * emit whichever items the server sends. Current implementation also uses
     * blocking socket API which blocks until there are some data on the socket.
     * Source processors are required to return control to be able to do the
     * snapshot. So if there are no data on socket, the snapshot will be
     * delayed and block the job.
     */
    @Nonnull
    public static Source<String> streamSocket(
            @Nonnull String host, int port, @Nonnull Charset charset
    ) {
        return fromProcessor("streamSocket(" + host + ':' + port + ')',
                StreamSocketP.supplier(host, port, charset.name()));
    }

    /**
     * A source that emits lines from files in a directory (but not its
     * subdirectories. The files must not change while being read; if they do,
     * the behavior is unspecified.
     * <p>
     * To be useful, the source should be configured to read data local to each
     * member. For example, if the pathname resolves to a shared network
     * filesystem visible by multiple members, they will emit duplicate data.
     * <p>
     * This source does not save any state to snapshot. If the job is restarted,
     * all entries will be emitted again.
     * <p>
     * Any {@code IOException} will cause the job to fail.
     *
     * @param directory parent directory of the files
     * @param charset charset to use to decode the files
     * @param glob the globbing mask, see {@link
     *             java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     *             Use {@code "*"} for all files.
     */
    @Nonnull
    public static Source<String> readFiles(
            @Nonnull String directory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return fromProcessor("readFiles(" + directory + '/' + glob + ')',
                ReadFilesP.supplier(directory, charset.name(), glob)
        );
    }

    /**
     * Convenience for {@link #readFiles(String, Charset, String) readFiles(directory, UTF_8, "*")}.
     */
    public static Source<String> readFiles(@Nonnull String directory) {
        return readFiles(directory, UTF_8, GLOB_WILDCARD);
    }

    /**
     * A source that emits a stream of lines of text coming from files in
     * the watched directory (but not its subdirectories). It will emit only
     * new contents added after startup: both new files and new content
     * appended to existing ones.
     * <p>
     * To be useful, the source should be configured to read data local to each
     * member. For example, if the pathname resolves to a shared network
     * filesystem visible by multiple members, they will emit duplicate data.
     * <p>
     * If, during the scanning phase, the source observes a file that doesn't
     * end with a newline, it will assume that there is a line just being
     * written. This line won't appear in its output.
     * <p>
     * The source completes when the directory is deleted. However, in order
     * to delete the directory, all files in it must be deleted and if you
     * delete a file that is currently being read from, the job may encounter
     * an {@code IOException}. The directory must be deleted on all nodes.
     * <p>
     * Any {@code IOException} will cause the job to fail.
     * <p>
     * This source does not save any state to snapshot. If the job is restarted,
     * lines added after the restart will be emitted, which gives at-most-once
     * behavior.
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
     *             Use {@code "*"} for all files.
     */
    public static Source<String> streamFiles(
            @Nonnull String watchedDirectory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return fromProcessor("streamFiles(" + watchedDirectory + '/' + glob + ')',
                StreamFilesP.supplier(watchedDirectory, charset.name(), glob)
        );
    }

    /**
     * Convenience for {@link #streamFiles(String, Charset, String)
     * streamFiles(watchedDirectory, UTF_8, "*")}.
     */
    public static Source<String> streamFiles(@Nonnull String watchedDirectory) {
        return streamFiles(watchedDirectory, UTF_8, GLOB_WILDCARD);
    }
}
