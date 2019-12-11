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

package com.hazelcast.jet.pipeline;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;

import javax.annotation.Nonnull;

/**
 * Utility class with methods that create several useful {@link ServiceFactory
 * service factories}.
 *
 * @since 3.0
 */
public final class ServiceFactories {

    private ServiceFactories() { }

    /**
     * Returns a factory that provides a {@link ReplicatedMap} as the service
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
     * p.readFrom( /* a batch or streaming source &#42;/ )
     *  .mapUsingService(replicatedMapService("fooMapName"),
     *      (map, item) -> tuple2(item, map.get(item.getKey())))
     *  .destroyFn(ReplicatedMap::destroy);
     * </pre>
     *
     * @param mapName name of the {@link ReplicatedMap} to use as the service
     * @param <K> type of the map key
     * @param <V> type of the map value
     *
     * @since 3.0
     */
    @Nonnull
    public static <K, V> ServiceFactory<?, ReplicatedMap<K, V>> replicatedMapService(@Nonnull String mapName) {
        return ServiceFactory.withCreateContainerFn(ctx -> ctx.jetInstance().<K, V>getReplicatedMap(mapName))
                             .withCreateServiceFn((ctx, map) -> map);
    }

    /**
     * Returns a factory that provides an {@link IMap} as the service. This
     * is useful if you are enriching an event stream with the data stored in
     * the Hazelcast Jet cluster. Unlike in a {@code hashJoin} transformation,
     * the data in the map can change while the job is running so you can keep
     * the enriching dataset up-to-date.
     * <p>
     * Instead of using this factory, you can call {@link
     * GeneralStage#mapUsingIMap(IMap, FunctionEx, BiFunctionEx)} or {@link
     * GeneralStageWithKey#mapUsingIMap(IMap, BiFunctionEx)}.
     * <p>
     * If you plan to use a sync method on the map, call {@link
     * ServiceFactory#toNonCooperative()} on the returned factory.
     *
     * @param mapName name of the map used as service
     * @param <K> key type
     * @param <V> value type
     * @return the service factory
     *
     * @since 3.0
     */
    @Nonnull
    public static <K, V> ServiceFactory<?, IMap<K, V>> iMapService(@Nonnull String mapName) {
        return ServiceFactory.withCreateContainerFn(ctx -> ctx.jetInstance().<K, V>getMap(mapName))
                             .withCreateServiceFn((ctx, map) -> map);
    }

    /**
     * TODO
     * @param createServiceFn
     * @param destroyServiceFn
     * @param <S>
     * @return
     */
    public static <S> ServiceFactory<?, S> perInstanceService(
            @Nonnull SupplierEx<S> createServiceFn,
            @Nonnull ConsumerEx<S> destroyServiceFn
    ) {
        return ServiceFactory.withCreateContainerFn(c -> createServiceFn.get())
                             .withCreateServiceFn((ctx, c) -> c)
                             .withDestroyContainerFn(destroyServiceFn);
    }

    /**
     * TODO
     * @param createServiceFn
     * @param destroyServiceFn
     * @param <S>
     * @return
     */
    public static <S> ServiceFactory<?, S> perProcessorService(
            @Nonnull SupplierEx<? extends S> createServiceFn,
            @Nonnull ConsumerEx<? super S> destroyServiceFn
    ) {
        return ServiceFactory.<Void>withCreateContainerFn(c -> null)
                .<S>withCreateServiceFn((ctx, c) -> createServiceFn.get())
                .withDestroyServiceFn(destroyServiceFn)
                .withDestroyContainerFn(ConsumerEx.noop());
    }
}
