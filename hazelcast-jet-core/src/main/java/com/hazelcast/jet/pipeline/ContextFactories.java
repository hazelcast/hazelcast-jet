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
 * Utility class with methods that create several useful kinds of {@link
 * ContextFactory context factories}.
 */
public final class ContextFactories {

    private ContextFactories() { }

    /**
     * Returns a factory that provides a {@link ReplicatedMap} as the context
     * object. A replicated map is a particularly good choice if you are
     * enriching an event stream with the data stored in the Hazelcast Jet
     * cluster. Unlike in a {@code hashJoin} transformation, the data in the
     * map can change while the job is running so you can keep the enriching
     * dataset up-to-date. Unlike {@code IMap}, the data you access is local so
     * you won't do any blocking calls using it (important for performance).
     * <p>
     * If you want to destroy the map after the job finishes, call
     * {@code factory.destroyFn(ReplicatedMap::destroy)} on the object you get
     * from this method.
     * <p>
     * Example usage (without destroyFn):
     * <pre>
     * p.drawFrom( /* a batch or streaming source &#42;/ )
     *  .mapUsingContext(replicatedMapContext("fooMapName"),
     *      (map, item) -> tuple2(item, map.get(item.getKey())))
     *  .destroyFn(ReplicatedMap::destroy);
     * </pre>
     *
     * @param mapName name of the {@link ReplicatedMap} to use as the context
     * @param <K> type of the map key
     * @param <V> type of the map value
     */
    @Nonnull
    public static <K, V> ContextFactory<ReplicatedMap<K, V>> replicatedMapContext(@Nonnull String mapName) {
        return ContextFactory
                .withCreateFn(jet -> jet.getHazelcastInstance().getReplicatedMap(mapName));
    }

    /**
     * Returns a factory that provides an {@link IMapJet} as the context. This
     * is useful if you are enriching an event stream with the data stored in
     * the Hazelcast Jet cluster. Unlike in a {@code hashJoin} transformation,
     * the data in the map can change while the job is running so you can keep
     * the enriching dataset up-to-date.
     * <p>
     * The downside of accessing an {@code IMap} in a Jet job is that it's a
     * partitioned data structure, so you'll be accessing remote data and
     * causing blocking in the stream processing pipeline. This will
     * significantly reduce the maximum throughput of the job. If you need more
     * throughput, consider using a {@link #replicatedMapContext(String)
     * ReplicatedMap} as the context or, alternatively, enable the near-cache on
     * the {@code IMap}.
     * <p>
     * If you want to destroy the map after the job finishes, call {@code
     * factory.destroyFn(IMap::destroy)} on the object you get from this
     * method.
     * <p>
     * Example usage (without destroyFn):
     * <pre>
     * p.drawFrom( /* a batch or streaming source &#42;/ )
     *  .mapUsingContext(iMapContext("fooMapName"),
     *      (map, item) -> tuple2(item, map.get(item.getKey())));
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
                .withCreateFn(jet -> jet.<K, V>getMap(mapName))
                .shareLocally()
                .nonCooperative();
    }

}
