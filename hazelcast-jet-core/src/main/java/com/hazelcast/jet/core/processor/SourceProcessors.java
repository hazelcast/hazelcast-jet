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

package com.hazelcast.jet.core.processor;

import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.ToResultSetFunction;
import com.hazelcast.jet.impl.connector.ConvenientSourceP;
import com.hazelcast.jet.impl.connector.ConvenientSourceP.SourceBufferConsumerSide;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.impl.connector.ReadIListP;
import com.hazelcast.jet.impl.connector.ReadJdbcP;
import com.hazelcast.jet.impl.connector.ReadWithPartitionIteratorP;
import com.hazelcast.jet.impl.connector.StreamEventJournalP;
import com.hazelcast.jet.impl.connector.StreamFilesP;
import com.hazelcast.jet.impl.connector.StreamJmsP;
import com.hazelcast.jet.impl.connector.StreamSocketP;
import com.hazelcast.jet.impl.pipeline.SourceBufferImpl;
import com.hazelcast.jet.pipeline.FileSourceBuilder;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import com.hazelcast.jet.pipeline.SourceBuilder.TimestampedSourceBuffer;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;

import javax.annotation.Nonnull;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.sql.ResultSet;
import java.util.Map.Entry;
import java.util.function.BiConsumer;

import static com.hazelcast.jet.Util.cacheEventToEntry;
import static com.hazelcast.jet.Util.cachePutEvents;
import static com.hazelcast.jet.Util.mapEventToEntry;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.impl.connector.StreamEventJournalP.streamRemoteCacheSupplier;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.util.Preconditions.checkNotNegative;

/**
 * Static utility class with factories of source processors (the DAG
 * entry points). For other kinds for a vertices refer to the {@link
 * com.hazelcast.jet.core.processor package-level documentation}.
 */
public final class SourceProcessors {

    private SourceProcessors() {
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#map(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readMapP(@Nonnull String mapName) {
        return ReadWithPartitionIteratorP.readMapSupplier(mapName);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#map(String, Predicate, Projection)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier readMapP(
            @Nonnull String mapName,
            @Nonnull Predicate<? super K, ? super V> predicate,
            @Nonnull Projection<? super Entry<K, V>, ? extends T> projectionFn
    ) {
        return ReadWithPartitionIteratorP.readMapSupplier(mapName, predicate, projectionFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#map(String, Predicate, DistributedFunction)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier readMapP(
            @Nonnull String mapName,
            @Nonnull Predicate<? super K, ? super V> predicate,
            @Nonnull DistributedFunction<? super Entry<K, V>, ? extends T> projectionFn
    ) {
        return ReadWithPartitionIteratorP.readMapSupplier(mapName, predicate, toProjection(projectionFn));
    }


    /**
     * Returns a supplier of processors for
     * {@link Sources#mapJournal(String, JournalInitialPosition)} )}.
     */
    @Nonnull
    public static <K, V> ProcessorMetaSupplier streamMapP(
            @Nonnull String mapName,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super Entry<K, V>> eventTimePolicy
    ) {
        return streamMapP(mapName, mapPutEvents(), mapEventToEntry(), initialPos, eventTimePolicy);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#mapJournal(String, DistributedPredicate, DistributedFunction, JournalInitialPosition)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier streamMapP(
            @Nonnull String mapName,
            @Nonnull DistributedPredicate<? super EventJournalMapEvent<K, V>> predicateFn,
            @Nonnull DistributedFunction<? super EventJournalMapEvent<K, V>, ? extends T> projectionFn,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        checkSerializable(predicateFn, "predicateFn");
        checkSerializable(projectionFn, "projectionFn");

        return StreamEventJournalP.streamMapSupplier(mapName, predicateFn, projectionFn, initialPos, eventTimePolicy);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteMap(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readRemoteMapP(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return ReadWithPartitionIteratorP.readRemoteMapSupplier(mapName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteMap(String, ClientConfig, Predicate, Projection)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier readRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull Predicate<? super K, ? super V> predicate,
            @Nonnull Projection<? super Entry<K, V>, ? extends T> projection
    ) {
        return ReadWithPartitionIteratorP.readRemoteMapSupplier(mapName, clientConfig, projection, predicate);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteMap(String, ClientConfig, Predicate, DistributedFunction)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier readRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull Predicate<? super K, ? super V> predicate,
            @Nonnull DistributedFunction<? super Entry<K, V>, ? extends T> projectionFn
    ) {
        return ReadWithPartitionIteratorP.readRemoteMapSupplier(
                mapName, clientConfig, toProjection(projectionFn), predicate
        );
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteMapJournal(String, ClientConfig, JournalInitialPosition)}.
     */
    @Nonnull
    public static <K, V> ProcessorMetaSupplier streamRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super Entry<K, V>> eventTimePolicy
    ) {
        return streamRemoteMapP(mapName, clientConfig, mapPutEvents(), mapEventToEntry(), initialPos,
                eventTimePolicy);
    }

    /**
     * Returns a supplier of processors for {@link
     * Sources#remoteMapJournal(String, ClientConfig, DistributedPredicate,
     * DistributedFunction, JournalInitialPosition)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier streamRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedPredicate<? super EventJournalMapEvent<K, V>> predicateFn,
            @Nonnull DistributedFunction<? super EventJournalMapEvent<K, V>, ? extends T> projectionFn,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        return StreamEventJournalP.streamRemoteMapSupplier(
                mapName, clientConfig, predicateFn, projectionFn, initialPos, eventTimePolicy);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#cache(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readCacheP(@Nonnull String cacheName) {
        return ReadWithPartitionIteratorP.readCacheSupplier(cacheName);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#cacheJournal(String, JournalInitialPosition)}.
     */
    @Nonnull
    public static <K, V> ProcessorMetaSupplier streamCacheP(
            @Nonnull String cacheName,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super Entry<K, V>> eventTimePolicy
    ) {
        return streamCacheP(cacheName, cachePutEvents(), cacheEventToEntry(), initialPos, eventTimePolicy);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#cacheJournal(String,
     * DistributedPredicate, DistributedFunction, JournalInitialPosition)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier streamCacheP(
            @Nonnull String cacheName,
            @Nonnull DistributedPredicate<? super EventJournalCacheEvent<K, V>> predicateFn,
            @Nonnull DistributedFunction<? super EventJournalCacheEvent<K, V>, ? extends T> projectionFn,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        return StreamEventJournalP.streamCacheSupplier(cacheName, predicateFn, projectionFn, initialPos,
                eventTimePolicy);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteCache(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readRemoteCacheP(
            @Nonnull String cacheName, @Nonnull ClientConfig clientConfig
    ) {
        return ReadWithPartitionIteratorP.readRemoteCacheSupplier(cacheName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteCacheJournal(String, ClientConfig, JournalInitialPosition)}.
     */
    @Nonnull
    public static <K, V> ProcessorMetaSupplier streamRemoteCacheP(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super Entry<K, V>> eventTimePolicy
    ) {
        return streamRemoteCacheP(
                cacheName, clientConfig, cachePutEvents(), cacheEventToEntry(), initialPos, eventTimePolicy);
    }

    /**
     * Returns a supplier of processors for {@link
     * Sources#remoteCacheJournal(String, ClientConfig,
     * DistributedPredicate, DistributedFunction, JournalInitialPosition)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier streamRemoteCacheP(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedPredicate<? super EventJournalCacheEvent<K, V>> predicateFn,
            @Nonnull DistributedFunction<? super EventJournalCacheEvent<K, V>, ? extends T> projectionFn,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        return streamRemoteCacheSupplier(
                cacheName, clientConfig, predicateFn, projectionFn, initialPos, eventTimePolicy);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#list(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readListP(@Nonnull String listName) {
        return ReadIListP.metaSupplier(listName, null);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteList(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readRemoteListP(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return ReadIListP.metaSupplier(listName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#socket(String, int, Charset)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamSocketP(
            @Nonnull String host, int port, @Nonnull Charset charset
    ) {
        return StreamSocketP.supplier(host, port, charset.name());
    }

    /**
     * Returns a supplier of processors for {@link Sources#filesBuilder}.
     * See {@link FileSourceBuilder#build} for more details.
     */
    @Nonnull
    public static <R> ProcessorMetaSupplier readFilesP(
            @Nonnull String directory,
            @Nonnull Charset charset,
            @Nonnull String glob,
            boolean sharedFileSystem,
            @Nonnull DistributedBiFunction<? super String, ? super String, ? extends R> mapOutputFn
    ) {
        checkSerializable(mapOutputFn, "mapOutputFn");

        String charsetName = charset.name();
        return ReadFilesP.metaSupplier(directory, glob, sharedFileSystem,
                path -> Files.lines(path, Charset.forName(charsetName)),
                mapOutputFn);
    }

    /**
     * Returns a supplier of processors for {@link Sources#filesBuilder}.
     * See {@link FileSourceBuilder#buildWatcher} for more details.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamFilesP(
            @Nonnull String watchedDirectory,
            @Nonnull Charset charset,
            @Nonnull String glob,
            boolean sharedFileSystem,
            @Nonnull DistributedBiFunction<? super String, ? super String, ?> mapOutputFn
    ) {
        checkSerializable(mapOutputFn, "mapOutputFn");

        return StreamFilesP.metaSupplier(watchedDirectory, charset.name(), glob, sharedFileSystem, mapOutputFn);
    }

    /**
     * Returns a supplier of processors for {@link Sources#jmsQueueBuilder}.
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier streamJmsQueueP(
            @Nonnull DistributedSupplier<? extends Connection> connectionSupplier,
            @Nonnull DistributedFunction<? super Connection, ? extends Session> sessionFn,
            @Nonnull DistributedFunction<? super Session, ? extends MessageConsumer> consumerFn,
            @Nonnull DistributedConsumer<? super Session> flushFn,
            @Nonnull DistributedFunction<? super Message, ? extends T> projectionFn,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy) {
        return ProcessorMetaSupplier.of(
                StreamJmsP.supplier(connectionSupplier, sessionFn, consumerFn, flushFn, projectionFn, eventTimePolicy),
                StreamJmsP.PREFERRED_LOCAL_PARALLELISM);
    }

    /**
     * Returns a supplier of processors for {@link Sources#jmsTopicBuilder}.
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier streamJmsTopicP(
            @Nonnull DistributedSupplier<? extends Connection> connectionSupplier,
            @Nonnull DistributedFunction<? super Connection, ? extends Session> sessionFn,
            @Nonnull DistributedFunction<? super Session, ? extends MessageConsumer> consumerFn,
            @Nonnull DistributedConsumer<? super Session> flushFn,
            @Nonnull DistributedFunction<? super Message, ? extends T> projectionFn,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                StreamJmsP.supplier(connectionSupplier, sessionFn, consumerFn, flushFn, projectionFn, eventTimePolicy));
    }

    /**
     * Returns a supplier of processors for {@link Sources#jdbc(
     * DistributedSupplier, ToResultSetFunction, DistributedFunction)}.
     */
    public static <T> ProcessorMetaSupplier readJdbcP(
            @Nonnull DistributedSupplier<? extends java.sql.Connection> connectionSupplier,
            @Nonnull ToResultSetFunction resultSetFn,
            @Nonnull DistributedFunction<? super ResultSet, ? extends T> mapOutputFn
    ) {
        checkSerializable(connectionSupplier, "connectionSupplier");
        checkSerializable(resultSetFn, "resultSetFn");
        checkSerializable(mapOutputFn, "mapOutputFn");
        return ReadJdbcP.supplier(connectionSupplier, resultSetFn, mapOutputFn);
    }

    /**
     * Returns a supplier of processors for {@link
     * Sources#jdbc(String, String, DistributedFunction)}.
     */
    public static <T> ProcessorMetaSupplier readJdbcP(
            @Nonnull String connectionURL,
            @Nonnull String query,
            @Nonnull DistributedFunction<? super ResultSet, ? extends T> mapOutputFn
    ) {
        checkSerializable(mapOutputFn, "mapOutputFn");
        return ReadJdbcP.supplier(connectionURL, query, mapOutputFn);
    }

    private static <I, O> Projection<I, O> toProjection(DistributedFunction<I, O> projectionFn) {
        return new Projection<I, O>() {
            @Override public O transform(I input) {
                return projectionFn.apply(input);
            }
        };
    }

    /**
     * Returns a supplier of processors for a source that the user can create
     * using the {@link SourceBuilder}. This variant creates a source that
     * emits items without timestamps.
     *
     * @param createFn function that creates the source's state object
     * @param fillBufferFn function that fills Jet's buffer with items to emit
     * @param destroyFn function that cleans up the resources held by the state object
     * @param preferredLocalParallelism preferred local parallelism of the source vertex. Special values:
     *                                  {@value Vertex#LOCAL_PARALLELISM_USE_DEFAULT} ->
     *                                  use the cluster's default local parallelism;
     *                                  0 -> create a single processor for the entire cluster (total parallelism = 1)
     * @param <S> type of the source's state object
     * @param <T> type of items the source emits
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public static <S, T> ProcessorMetaSupplier convenientSourceP(
            @Nonnull DistributedFunction<? super Processor.Context, ? extends S> createFn,
            @Nonnull DistributedBiConsumer<? super S, ? super SourceBuffer<T>> fillBufferFn,
            @Nonnull DistributedConsumer<? super S> destroyFn,
            int preferredLocalParallelism
    ) {
        checkSerializable(createFn, "createFn");
        checkSerializable(fillBufferFn, "fillBufferFn");
        checkSerializable(destroyFn, "destroyFn");
        checkNotNegative(preferredLocalParallelism + 1, "preferredLocalParallelism must >= -1");
        ProcessorSupplier procSup = ProcessorSupplier.of(
                () -> new ConvenientSourceP<S, T>(
                        createFn,
                        (BiConsumer<? super S, ? super SourceBufferConsumerSide<?>>) fillBufferFn,
                        destroyFn,
                        new SourceBufferImpl.Plain<>(),
                        null));
        return preferredLocalParallelism != 0
                ? ProcessorMetaSupplier.of(procSup, preferredLocalParallelism)
                : ProcessorMetaSupplier.forceTotalParallelismOne(procSup);
    }

    /**
     * Returns a supplier of processors for a source that the user can create
     * using the {@link SourceBuilder}. This variant creates a source that
     * emits timestamped events.
     *
     * @param createFn function that creates the source's state object
     * @param fillBufferFn function that fills Jet's buffer with items to emit
     * @param eventTimePolicy parameters for watermark generation
     * @param destroyFn function that cleans up the resources held by the state object
     * @param preferredLocalParallelism preferred local parallelism of the source vertex. Special values:
     *                                  {@value Vertex#LOCAL_PARALLELISM_USE_DEFAULT} ->
     *                                  use the cluster's default local parallelism;
     *                                  0 -> create a single processor for the entire cluster (total parallelism = 1)
     * @param <S> type of the source's state object
     * @param <T> type of items the source emits
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public static <S, T> ProcessorMetaSupplier convenientTimestampedSourceP(
            @Nonnull DistributedFunction<? super Processor.Context, ? extends S> createFn,
            @Nonnull DistributedBiConsumer<? super S, ? super TimestampedSourceBuffer<T>> fillBufferFn,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy,
            @Nonnull DistributedConsumer<? super S> destroyFn,
            int preferredLocalParallelism
    ) {
        checkSerializable(createFn, "createFn");
        checkSerializable(fillBufferFn, "fillBufferFn");
        checkSerializable(destroyFn, "destroyFn");
        checkNotNegative(preferredLocalParallelism + 1, "preferredLocalParallelism must >= -1");
        ProcessorSupplier procSup = ProcessorSupplier.of(
                () -> new ConvenientSourceP<>(
                        createFn,
                        (BiConsumer<? super S, ? super SourceBufferConsumerSide<?>>) fillBufferFn,
                        destroyFn,
                        new SourceBufferImpl.Timestamped<>(),
                        eventTimePolicy
                ));
        return preferredLocalParallelism > 0
                ? ProcessorMetaSupplier.of(procSup, preferredLocalParallelism)
                : ProcessorMetaSupplier.forceTotalParallelismOne(procSup);
    }
}
