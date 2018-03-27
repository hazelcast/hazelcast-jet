/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.impl.pipeline.transform.BatchSourceTransform;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.projection.Projection;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicate;

import javax.annotation.Nonnull;
import java.io.File;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import static com.hazelcast.jet.Util.cacheEventToEntry;
import static com.hazelcast.jet.Util.cachePutEvents;
import static com.hazelcast.jet.Util.mapEventToEntry;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.core.processor.SourceProcessors.readCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readFilesP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readListP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readRemoteCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readRemoteListP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readRemoteMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamFilesP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamRemoteCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamRemoteMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamSocketP;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Contains factory methods for various types of pipeline sources. To start
 * building a pipeline, pass a source to {@link Pipeline#drawFrom(BatchSource)}
 * and you will obtain the initial {@link BatchStage}. You can then
 * attach further stages to it.
 * <p>
 * The same pipeline may contain more than one source, each starting its
 * own branch. The branches may be merged with multiple-input transforms
 * such as co-group and hash-join.
 * <p>
 * The default local parallelism for sources in this class is 1 or 2, check the
 * documentation of individual methods.
 */
public final class Sources {

    private static final String GLOB_WILDCARD = "*";

    private Sources() {
    }

    /**
     * Returns a bounded (batch) source constructed directly from the given
     * Core API processor meta-supplier.
     *
     * @param sourceName user-friendly source name
     * @param metaSupplier the processor meta-supplier
     */
    @Nonnull
    public static <T> BatchSource<T> batchFromProcessor(
            @Nonnull String sourceName,
            @Nonnull ProcessorMetaSupplier metaSupplier
    ) {
        return new BatchSourceTransform<>(sourceName, metaSupplier);
    }

    /**
     * Returns an unbounded (event stream) source that will use the supplied
     * function to create processor meta-suppliers as required by the Core API.
     * Jet will call the function you supply with the watermark generation
     * parameters and it must return a meta-supplier of processors that will
     * act according to these parameters and emit the watermark items as they
     * specify.
     * <p>
     * If you are implementing a custom source processor, be sure to check out
     * the {@link com.hazelcast.jet.core.WatermarkSourceUtil} class that will
     * help you correctly implement watermark item emission.
     *
     * @param sourceName user-friendly source name
     * @param metaSupplierFn factory of processor meta-suppliers
     */
    @Nonnull
    public static <T> StreamSource<T> streamFromProcessorWithWatermarks(
            @Nonnull String sourceName,
            @Nonnull Function<WatermarkGenerationParams<T>, ProcessorMetaSupplier> metaSupplierFn
    ) {
        return new StreamSourceTransform<>(sourceName, metaSupplierFn, true);
    }

    /**
     * Returns an unbounded (event stream) source constructed directly from the given
     * Core API processor meta-supplier.
     *
     * @param sourceName user-friendly source name
     * @param metaSupplier the processor meta-supplier
     */
    @Nonnull
    public static <T> StreamSource<T> streamFromProcessor(
            @Nonnull String sourceName,
            @Nonnull ProcessorMetaSupplier metaSupplier
    ) {
        return new StreamSourceTransform<>(sourceName, w -> metaSupplier, false);
    }

    /**
     * Returns a source that fetches entries from a local Hazelcast {@code IMap}
     * with the specified name and emits them as {@code Map.Entry}. It leverages
     * data locality by making each of the underlying processors fetch only those
     * entries that are stored on the member where it is running.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     * <p>
     * The default local parallelism for this processor is 2 (or 1 if just 1
     * CPU is available).
     */
    @Nonnull
    public static <K, V> BatchSource<Entry<K, V>> map(@Nonnull String mapName) {
        return batchFromProcessor("mapSource(" + mapName + ')', readMapP(mapName));
    }

    /**
     * Returns a source that fetches entries from a local Hazelcast {@code
     * IMap} with the specified name. By supplying a {@code predicate} and
     * {@code projection} here instead of in separate {@code map/filter}
     * transforms you allow the source to apply these functions early, before
     * generating any output, with the potential of significantly reducing
     * data traffic. If your data is stored in the IMDG using the <a href=
     *     "http://docs.hazelcast.org/docs/3.9/manual/html-single/index.html#implementing-portable-serialization">
     * portable serialization format</a>, there are additional optimizations
     * available when using {@link
     *     com.hazelcast.projection.Projections#singleAttribute(String)
     * Projections.singleAttribute()} and {@link
     *     com.hazelcast.projection.Projections#multiAttribute(String...)
     * Projections.multiAttribute()}) to create your projection instance and
     * using the {@link com.hazelcast.jet.GenericPredicates} factory or
     * {@link com.hazelcast.query.PredicateBuilder PredicateBuilder} to create
     * the predicate. In this case Jet can test the predicate and apply the
     * projection without deserializing the whole object.
     * <p>
     * The source leverages data locality by making each of the underlying
     * processors fetch only those entries that are stored on the member where
     * it is running.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     * <p>
     * The default local parallelism for this processor is 2 (or 1 if just 1
     * CPU is available).
     *
     * <h4>Predicate/projection class requirements</h4>
     *
     * The classes implementing {@code predicate} and {@code projection} need
     * to be available on the cluster's classpath, or loaded using
     * <em>Hazelcast User Code Deployment</em>. It's not enough to add them to
     * job classpath in {@link com.hazelcast.jet.config.JobConfig}. Same is
     * true for the class of the objects stored in the map itself. If you
     * cannot fulfill these conditions, use {@link #map(String)} and add a
     * subsequent {@link GeneralStage#map map} or {@link GeneralStage#filter
     * filter} stage.
     *
     * @param mapName the name of the map
     * @param predicate the predicate to filter the events. If you want to specify just the
     *                  projection, use {@link
     *                  com.hazelcast.jet.GenericPredicates#alwaysTrue()} as a pass-through
     *                  predicate
     * @param projection the projection to map the events. If the projection returns a {@code
     *                   null} for an item, that item will be filtered out. If you want to
     *                   specify just the predicate, use {@link Projections#identity()}.
     * @param <T> type of emitted item
     */
    @Nonnull
    public static <T, K, V> BatchSource<T> map(
            @Nonnull String mapName,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull Projection<Entry<K, V>, T> projection
    ) {
        return batchFromProcessor("mapSource(" + mapName + ')', readMapP(mapName, predicate, projection));
    }

    /**
     * Convenience for {@link #map(String, Predicate, Projection)}
     * which uses a {@link DistributedFunction} as the projection function.
     */
    @Nonnull
    public static <T, K, V> BatchSource<T> map(
            @Nonnull String mapName,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull DistributedFunction<Map.Entry<K, V>, T> projectionFn
    ) {
        return batchFromProcessor("mapSource(" + mapName + ')', readMapP(mapName, predicate, projectionFn));
    }

    /**
     * Returns a source that will stream {@link EventJournalMapEvent}s of the
     * Hazelcast {@code IMap} with the specified name. By supplying a {@code
     * predicate} and {@code projection} here instead of in separate {@code
     * map/filter} transforms you allow the source to apply these functions
     * early, before generating any output, with the potential of significantly
     * reducing data traffic.
     * <p>
     * The source leverages data locality by making each of the underlying
     * processors fetch only those entries that are stored on the member where
     * it is running.
     * <p>
     * To use an {@code IMap} as a streaming source, you must {@link
     * com.hazelcast.config.EventJournalConfig configure the event journal}
     * for it. The journal has fixed capacity and will drop events if it
     * overflows.
     * <p>
     * The source saves the journal offset to the snapshot. If the job
     * restarts, it starts emitting from the saved offset with an
     * exactly-once guarantee (unless the journal has overflowed).
     * <p>
     * The default local parallelism for this processor is 2 (or 1 if just 1
     * CPU is available).
     *
     * <h4>Predicate/projection class requirements</h4>
     *
     * The classes implementing {@code predicateFn} and {@code projectionFn}
     * need to be available on the cluster's classpath, or loaded using
     * <em>Hazelcast User Code Deployment</em>. It's not enough to add them to
     * job classpath in {@link com.hazelcast.jet.config.JobConfig}. Same is
     * true for the class of the objects stored in the map itself. If you
     * cannot fulfill these conditions, use {@link #mapJournal(String,
     * JournalInitialPosition)} and add a subsequent {@link GeneralStage#map
     * map} or {@link GeneralStage#filter filter} stage.
     *
     * @param mapName the name of the map
     * @param predicateFn the predicate to filter the events. If you want to specify just the
     *                    projection, use {@link Util#mapPutEvents} to pass only {@link
     *                    com.hazelcast.core.EntryEventType#ADDED ADDED} and {@link
     *                    com.hazelcast.core.EntryEventType#UPDATED UPDATED} events.
     * @param projectionFn the projection to map the events. If the projection returns a {@code
     *                     null} for an item, that item will be filtered out. You may use {@link
     *                     Util#mapEventToEntry()} to extract just the key and the new value.
     * @param initialPos describes which event to start receiving from
     * @param <T> type of emitted item
     */
    @Nonnull
    public static <T, K, V> StreamSource<T> mapJournal(
            @Nonnull String mapName,
            @Nonnull DistributedPredicate<EventJournalMapEvent<K, V>> predicateFn,
            @Nonnull DistributedFunction<EventJournalMapEvent<K, V>, T> projectionFn,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return streamFromProcessorWithWatermarks("mapJournalSource(" + mapName + ')',
                w -> streamMapP(mapName, predicateFn, projectionFn, initialPos, w));
    }

    /**
     * Convenience for {@link #mapJournal(String, DistributedPredicate,
     * DistributedFunction, JournalInitialPosition)}
     * which will pass only {@link com.hazelcast.core.EntryEventType#ADDED
     * ADDED} and {@link com.hazelcast.core.EntryEventType#UPDATED UPDATED}
     * events and will project the event's key and new value into a {@code
     * Map.Entry}.
     */
    @Nonnull
    public static <K, V> StreamSource<Entry<K, V>> mapJournal(
            @Nonnull String mapName,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return mapJournal(mapName, mapPutEvents(), mapEventToEntry(), initialPos);
    }

    /**
     * Returns a source that fetches entries from the Hazelcast {@code IMap}
     * with the specified name in a remote cluster identified by the supplied
     * {@code ClientConfig} and emits them as {@code Map.Entry}.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     * <p>
     * The default local parallelism for this processor is 1.
     */
    @Nonnull
    public static <K, V> BatchSource<Entry<K, V>> remoteMap(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig
    ) {
        return batchFromProcessor("remoteMapSource(" + mapName + ')', readRemoteMapP(mapName, clientConfig));
    }

    /**
     * Returns a source that fetches entries from a remote Hazelcast {@code
     * IMap} with the specified name in a remote cluster identified by the
     * supplied {@code ClientConfig}. By supplying a {@code predicate} and
     * {@code projection} here instead of in separate {@code map/filter}
     * transforms you allow the source to apply these functions early, before
     * generating any output, with the potential of significantly reducing
     * data traffic. If your data is stored in the IMDG using the <a href=
     *     "http://docs.hazelcast.org/docs/3.9/manual/html-single/index.html#implementing-portable-serialization">
     * portable serialization format</a>, there are additional optimizations
     * available when using {@link
     *     com.hazelcast.projection.Projections#singleAttribute(String)
     * Projections.singleAttribute()} and {@link
     *     com.hazelcast.projection.Projections#multiAttribute(String...)
     * Projections.multiAttribute()}) to create your projection instance and
     * using the {@link com.hazelcast.jet.GenericPredicates} factory or
     * {@link com.hazelcast.query.PredicateBuilder PredicateBuilder} to create
     * the predicate. In this case Jet can test the predicate and apply the
     * projection without deserializing the whole object.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     * <p>
     * The default local parallelism for this processor is 1.
     *
     * <h4>Predicate/projection class requirements</h4>
     *
     * The classes implementing {@code predicate} and {@code projection} need
     * to be available on the remote cluster's classpath, or loaded using
     * <em>Hazelcast User Code Deployment</em>. It's not enough to add them to
     * job classpath in {@link com.hazelcast.jet.config.JobConfig}. Same is
     * true for the class of the objects stored in the map itself. If you
     * cannot fulfill these conditions, use {@link #remoteMap(String,
     * ClientConfig)} and add a subsequent {@link GeneralStage#map map} or
     * {@link GeneralStage#filter filter} stage.
     *
     * @param mapName the name of the map
     * @param predicate the predicate to filter the events. If you want to specify just the
     *                  projection, use {@link
     *                  com.hazelcast.jet.GenericPredicates#alwaysTrue()} as a pass-through
     *                  predicate
     * @param projection the projection to map the events. If the projection returns a {@code
     *                   null} for an item, that item will be filtered out. If you want to
     *                   specify just the predicate, use {@link Projections#identity()}.
     * @param <T> type of emitted item
     */
    @Nonnull
    public static <T, K, V> BatchSource<T> remoteMap(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull Projection<Entry<K, V>, T> projection
    ) {
        return batchFromProcessor("remoteMapSource(" + mapName + ')',
                readRemoteMapP(mapName, clientConfig, predicate, projection));
    }

    /**
     * Convenience for {@link #remoteMap(String, ClientConfig, Predicate, Projection)}
     * which use a {@link DistributedFunction} as the projection function.
     */
    @Nonnull
    public static <T, K, V> BatchSource<T> remoteMap(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull DistributedFunction<Entry<K, V>, T> projectionFn
    ) {
        return batchFromProcessor("remoteMapSource(" + mapName + ')',
                readRemoteMapP(mapName, clientConfig, predicate, projectionFn));
    }

    /**
     * Returns a source that will stream the {@link EventJournalMapEvent}
     * events of the Hazelcast {@code IMap} with the specified name from a
     * remote cluster. By supplying a {@code predicate} and {@code projection}
     * here instead of in separate {@code map/filter} transforms you allow the
     * source to apply these functions early, before generating any output,
     * with the potential of significantly reducing data traffic.
     * <p>
     * To use an {@code IMap} as a streaming source, you must {@link
     * com.hazelcast.config.EventJournalConfig configure the event journal}
     * for it. The journal has fixed capacity and will drop events if it
     * overflows.
     * <p>
     * The source saves the journal offset to the snapshot. If the job
     * restarts, it starts emitting from the saved offset with an
     * exactly-once guarantee (unless the journal has overflowed).
     * <p>
     * The default local parallelism for this processor is 1.
     *
     * <h4>Predicate/projection class requirements</h4>
     *
     * The classes implementing {@code predicateFn} and {@code projectionFn}
     * need to be available on the remote cluster's classpath, or loaded using
     * <em>Hazelcast User Code Deployment</em>. It's not enough to add them to
     * job classpath in {@link com.hazelcast.jet.config.JobConfig}. Same is
     * true for the class of the objects stored in the map itself. If you
     * cannot fulfill these conditions, use {@link #remoteMapJournal(String,
     * ClientConfig, JournalInitialPosition)} and add a subsequent {@link
     * GeneralStage#map map} or {@link GeneralStage#filter filter} stage.
     *
     * @param mapName the name of the map
     * @param clientConfig configuration for the client to connect to the remote cluster
     * @param predicateFn the predicate to filter the events. You may use {@link Util#mapPutEvents}
     *                    to pass only {@link com.hazelcast.core.EntryEventType#ADDED
     *                    ADDED} and {@link com.hazelcast.core.EntryEventType#UPDATED UPDATED}
     *                    events.
     * @param projectionFn the projection to map the events. If the projection returns a {@code
     *                     null} for an item, that item will be filtered out. You may use {@link
     *                     Util#mapEventToEntry()} to extract just the key and the new value.
     * @param initialPos describes which event to start receiving from
     * @param <K> type of key
     * @param <V> type of value
     * @param <T> type of emitted item
     */
    @Nonnull
    public static <T, K, V> StreamSource<T> remoteMapJournal(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedPredicate<EventJournalMapEvent<K, V>> predicateFn,
            @Nonnull DistributedFunction<EventJournalMapEvent<K, V>, T> projectionFn,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return streamFromProcessorWithWatermarks("remoteMapJournalSource(" + mapName + ')',
                w -> streamRemoteMapP(mapName, clientConfig, predicateFn, projectionFn, initialPos, w));
    }

    /**
     * Convenience for {@link #remoteMapJournal(String, ClientConfig,
     * DistributedPredicate, DistributedFunction, JournalInitialPosition)}
     * which will pass only {@link com.hazelcast.core.EntryEventType#ADDED ADDED}
     * and {@link com.hazelcast.core.EntryEventType#UPDATED UPDATED} events and will
     * project the event's key and new value into a {@code Map.Entry}.
     */
    @Nonnull
    public static <K, V> StreamSource<Entry<K, V>> remoteMapJournal(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return remoteMapJournal(mapName, clientConfig, mapPutEvents(), mapEventToEntry(), initialPos);
    }

    /**
     * Returns a source that fetches entries from the Hazelcast {@code ICache}
     * with the specified name and emits them as {@code Map.Entry}. It
     * leverages data locality by making each of the underlying processors
     * fetch only those entries that are stored on the member where it is
     * running.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code ICache} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     * <p>
     * The default local parallelism for this processor is 2 (or 1 if just 1
     * CPU is available).
     */
    @Nonnull
    public static <K, V> BatchSource<Entry<K, V>> cache(@Nonnull String cacheName) {
        return batchFromProcessor("cacheSource(" + cacheName + ')', readCacheP(cacheName));
    }

    /**
     * Returns a source that will stream the {@link EventJournalCacheEvent}
     * events of the Hazelcast {@code ICache} with the specified name. By
     * supplying a {@code predicate} and {@code projection} here instead of
     * in separate {@code map/filter} transforms you allow the source to apply
     * these functions early, before generating any output, with the potential
     * of significantly reducing data traffic.
     * <p>
     * The source leverages data locality by making each of the underlying
     * processors fetch only those entries that are stored on the member where
     * it is running.
     * <p>
     * To use an {@code ICache} as a streaming source, you must {@link
     * com.hazelcast.config.EventJournalConfig configure the event journal}
     * for it. The journal has fixed capacity and will drop events if it
     * overflows.
     * <p>
     * The source saves the journal offset to the snapshot. If the job
     * restarts, it starts emitting from the saved offset with an
     * exactly-once guarantee (unless the journal has overflowed).
     * <p>
     * The default local parallelism for this processor is 2 (or 1 if just 1
     * CPU is available).
     *
     * <h4>Predicate/projection class requirements</h4>
     *
     * The classes implementing {@code predicateFn} and {@code projectionFn}
     * need to be available on the cluster's classpath, or loaded using
     * <em>Hazelcast User Code Deployment</em>. It's not enough to add them to
     * job classpath in {@link com.hazelcast.jet.config.JobConfig}. Same is
     * true for the class of the objects stored in the cache itself. If you
     * cannot fulfill these conditions, use {@link #cacheJournal(String,
     * JournalInitialPosition)} and add a subsequent {@link GeneralStage#map
     * map} or {@link GeneralStage#filter filter} stage.
     *
     * @param cacheName the name of the cache
     * @param predicateFn the predicate to filter the events. You may use {@link
     *                    Util#cachePutEvents()} to pass only {@link
     *                    com.hazelcast.cache.CacheEventType#CREATED CREATED} and {@link
     *                    com.hazelcast.cache.CacheEventType#UPDATED UPDATED} events.
     * @param projectionFn the projection to map the events. If the projection returns a {@code
     *                     null} for an item, that item will be filtered out. You may use {@link
     *                     Util#cacheEventToEntry()} to extract just the key and the new value.
     * @param initialPos describes which event to start receiving from
     * @param <T> type of emitted item
     */
    @Nonnull
    public static <T, K, V> StreamSource<T> cacheJournal(
            @Nonnull String cacheName,
            @Nonnull DistributedPredicate<EventJournalCacheEvent<K, V>> predicateFn,
            @Nonnull DistributedFunction<EventJournalCacheEvent<K, V>, T> projectionFn,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return streamFromProcessorWithWatermarks("cacheJournalSource(" + cacheName + ')',
                w -> streamCacheP(cacheName, predicateFn, projectionFn, initialPos, w)
        );
    }

    /**
     * Convenience for {@link #cacheJournal(String, DistributedPredicate,
     * DistributedFunction, JournalInitialPosition)}
     * which will pass only {@link com.hazelcast.cache.CacheEventType#CREATED
     * CREATED} and {@link com.hazelcast.cache.CacheEventType#UPDATED UPDATED}
     * events and will project the event's key and new value into a {@code
     * Map.Entry}.
     */
    @Nonnull
    public static <K, V> StreamSource<Entry<K, V>> cacheJournal(
            @Nonnull String cacheName,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return cacheJournal(cacheName, cachePutEvents(), cacheEventToEntry(), initialPos);
    }

    /**
     * Returns a source that fetches entries from the Hazelcast {@code ICache}
     * with the specified name in a remote cluster identified by the supplied
     * {@code ClientConfig} and emits them as {@code Map.Entry}.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code ICache} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     * <p>
     * The default local parallelism for this processor is 1.
     */
    @Nonnull
    public static <K, V> BatchSource<Entry<K, V>> remoteCache(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig
    ) {
        return batchFromProcessor(
                "remoteCacheSource(" + cacheName + ')', readRemoteCacheP(cacheName, clientConfig)
        );
    }

    /**
     * Returns a source that will stream the {@link EventJournalCacheEvent}
     * events of the Hazelcast {@code ICache} with the specified name from a
     * remote cluster. By supplying a {@code predicate} and {@code projection}
     * here instead of in separate {@code map/filter} transforms you allow the
     * source to apply these functions early, before generating any output,
     * with the potential of significantly reducing data traffic.
     * <p>
     * To use an {@code ICache} as a streaming source, you must {@link
     * com.hazelcast.config.EventJournalConfig configure the event journal}
     * for it. The journal has fixed capacity and will drop events if it
     * overflows.
     * <p>
     * The source saves the journal offset to the snapshot. If the job
     * restarts, it starts emitting from the saved offset with an
     * exactly-once guarantee (unless the journal has overflowed).
     * <p>
     * The default local parallelism for this processor is 1.
     *
     * <h4>Predicate/projection class requirements</h4>
     *
     * The classes implementing {@code predicateFn} and {@code projectionFn}
     * need to be available on the cluster's classpath, or loaded using
     * <em>Hazelcast User Code Deployment</em>. It's not enough to add them to
     * job classpath in {@link com.hazelcast.jet.config.JobConfig}. Same is
     * true for the class of the objects stored in the cache itself. If you
     * cannot fulfill these conditions, use {@link #remoteCacheJournal(String,
     * ClientConfig, JournalInitialPosition)} and add a subsequent {@link
     * GeneralStage#map map} or {@link GeneralStage#filter filter} stage.
     *
     * @param cacheName the name of the cache
     * @param clientConfig configuration for the client to connect to the remote cluster
     * @param predicateFn the predicate to filter the events. You may use {@link
     *                    Util#cachePutEvents()} to pass only {@link
     *                    com.hazelcast.cache.CacheEventType#CREATED CREATED} and {@link
     *                    com.hazelcast.cache.CacheEventType#UPDATED UPDATED} events.
     * @param projectionFn the projection to map the events. If the projection returns a {@code
     *                     null} for an item, that item will be filtered out. You may use {@link
     *                     Util#cacheEventToEntry()} to extract just the key and the new value.
     * @param initialPos describes which event to start receiving from
     * @param <T> type of emitted item
     */
    @Nonnull
    public static <T, K, V> StreamSource<T> remoteCacheJournal(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedPredicate<EventJournalCacheEvent<K, V>> predicateFn,
            @Nonnull DistributedFunction<EventJournalCacheEvent<K, V>, T> projectionFn,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return streamFromProcessorWithWatermarks("remoteCacheJournalSource(" + cacheName + ')',
                w -> streamRemoteCacheP(cacheName, clientConfig, predicateFn, projectionFn, initialPos, w));
    }

    /**
     * Convenience for {@link #remoteCacheJournal(String, ClientConfig,
     * DistributedPredicate, DistributedFunction, JournalInitialPosition)}
     * which will pass only
     * {@link com.hazelcast.cache.CacheEventType#CREATED CREATED}
     * and {@link com.hazelcast.cache.CacheEventType#UPDATED UPDATED}
     * events and will project the event's key and new value
     * into a {@code Map.Entry}.
     */
    @Nonnull
    public static <K, V> StreamSource<Entry<K, V>> remoteCacheJournal(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return remoteCacheJournal(cacheName, clientConfig, cachePutEvents(), cacheEventToEntry(), initialPos);
    }

    /**
     * Returns a source that emits items retrieved from a Hazelcast {@code
     * IList}. All elements are emitted on a single member &mdash; the one
     * where the entire list is stored by the IMDG.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * The default local parallelism for this processor is 1.
     */
    @Nonnull
    public static <T> BatchSource<T> list(@Nonnull String listName) {
        return batchFromProcessor("listSource(" + listName + ')', readListP(listName));
    }

    /**
     * Returns a source that emits items retrieved from a Hazelcast {@code
     * IList} in a remote cluster identified by the supplied {@code
     * ClientConfig}. All elements are emitted on a single member.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * The default local parallelism for this processor is 1.
     */
    @Nonnull
    public static <T> BatchSource<T> remoteList(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return batchFromProcessor("remoteListSource(" + listName + ')', readRemoteListP(listName, clientConfig));
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
     * The source does not save any state to snapshot. On job restart, it will
     * emit whichever items the server sends. The implementation uses
     * non-blocking API, the processor is cooperative.
     * <p>
     * The default local parallelism for this processor is 1.
     */
    @Nonnull
    public static StreamSource<String> socket(
            @Nonnull String host, int port, @Nonnull Charset charset
    ) {
        return streamFromProcessor(
                "socketSource(" + host + ':' + port + ')', streamSocketP(host, port, charset)
        );
    }

    /**
     * Convenience for {@link #socket socket(host, port, charset)} with
     * UTF-8 as the charset.
     *
     * @param host the hostname to connect to
     * @param port the port to connect to
     */
    @Nonnull
    public static StreamSource<String> socket(@Nonnull String host, int port) {
        return socket(host, port, UTF_8);
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
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * Any {@code IOException} will cause the job to fail.
     * <p>
     * The default local parallelism for this processor is 2 (or 1 if just 1
     * CPU is available).
     *
     * @param directory parent directory of the files
     * @param charset charset to use to decode the files
     * @param glob the globbing mask, see {@link
     *             java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     *             Use {@code "*"} for all files.
     * @param mapOutputFn function to create output items. Parameters are
     *                    {@code fileName} and {@code line}.
     */
    @Nonnull
    public static <R> BatchSource<R> files(
            @Nonnull String directory,
            @Nonnull Charset charset,
            @Nonnull String glob,
            @Nonnull DistributedBiFunction<String, String, ? extends R> mapOutputFn
    ) {
        return batchFromProcessor("filesSource(" + new File(directory, glob) + ')',
                readFilesP(directory, charset, glob, mapOutputFn));
    }

    /**
     * Convenience for {@link #files(String, Charset, String, DistributedBiFunction)
     * the full version of readFiles} which uses UTF-8 encoding, matches all
     * the files in the directory and emits lines of text in the files.
     */
    @Nonnull
    public static BatchSource<String> files(@Nonnull String directory) {
        return files(directory, UTF_8, GLOB_WILDCARD, (file, line) -> line);
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
     * The source does not save any state to snapshot. If the job is restarted,
     * lines added after the restart will be emitted, which gives at-most-once
     * behavior.
     * <p>
     * The default local parallelism for this processor is 2 (or 1 if just 1
     * CPU is available).
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
    @Nonnull
    public static StreamSource<String> fileWatcher(
            @Nonnull String watchedDirectory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return streamFromProcessor("fileWatcherSource(" + watchedDirectory + '/' + glob + ')',
                streamFilesP(watchedDirectory, charset, glob, (file, line) -> line)
        );
    }

    /**
     * Convenience for {@link #fileWatcher(String, Charset, String)
     * streamFiles(watchedDirectory, UTF_8, "*")}.
     */
    @Nonnull
    public static StreamSource<String> fileWatcher(@Nonnull String watchedDirectory) {
        return fileWatcher(watchedDirectory, UTF_8, GLOB_WILDCARD);
    }
}
