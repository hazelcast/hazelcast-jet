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

import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.jet.IMapJet;

import javax.annotation.Nonnull;

/**
 * Utility class with factory methods for several useful context factories, see
 * {@link ContextFactory}.
 */
public final class ContextFactories {

    private ContextFactories() { }

    /**
     * Returns context factory to use a {@link ReplicatedMap} with the given
     * {@code mapName} as a transformation context. It is useful for enrichment
     * based on data stored in the replicated map. Using a replicated map to
     * enrich is particularly useful in streaming jobs because it can be mutated
     * in parallel. In contrast to using {@link #iMapContext}
     * <p>
     * Example:
     *
     * <pre>
     *     p.drawFrom( /* a batch or streaming source &#42;/ )
     *      .mapUsingContext(replicatedMapContext("fooMapName"),
     *          (map, item) -> tuple2(item, map.get(item.getKey())));
     * </pre>
     *
     * @param mapName name of the {@link ReplicatedMap} used as context
     * @param <K> key type
     * @param <V> value type
     * @return the context factory
     */
    @Nonnull
    public static <K, V> ContextFactory<ReplicatedMap<K, V>> replicatedMapContext(@Nonnull String mapName) {
        return ContextFactory
                .withCreate(jet -> jet.getHazelcastInstance().getReplicatedMap(mapName));
    }

    /**
     * Returns context factory to use an {@link IMapJet} with the given {@code
     * mapName} as a transformation context. It is useful for enrichment based
     * on data stored in the IMap. Using a map to enrich is particularly useful
     * in streaming jobs because it can be mutated in parallel.
     * <p>
     * Keep in mind that the processor uses synchronous {@link IMapJet#get get}
     * method, which might involve network IO, so it's only appropriate for
     * low-traffic streams. You can use {@link #replicatedMapContext} instead or
     * configure near-cache.
     * <p>
     * If you want to destroy the map after the job finishes, call {@code
     * destroyFn(IMap::destroy)} on the returned object.
     * <p>
     * Example:
     *
     * <pre>
     *     p.drawFrom( /* a batch or streaming source &#42;/ )
     *      .mapUsingContext(iMapContext("fooMapName"),
     *          (map, item) -> tuple2(item, map.get(item.getKey())));
     * </pre>
     *
     * @param mapName name of the map used as context
     * @param <K> key type
     * @param <V> value type
     * @return the context factory
     */
    @Nonnull
    public static <K, V> ContextFactory<IMapJet<K, V>> iMapContext(@Nonnull String mapName) {
        return ContextFactory
                .withCreate(jet -> jet.<K, V>getMap(mapName))
                .shareLocally()
                .nonCooperative();
    }

}
