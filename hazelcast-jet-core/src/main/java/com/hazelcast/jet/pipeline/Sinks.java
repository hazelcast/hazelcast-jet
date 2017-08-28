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
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.impl.SinkImpl;
import com.hazelcast.jet.processor.SinkProcessors;

import javax.annotation.Nonnull;
import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class Sinks {

    private Sinks() {
    }

    /**
     * Returns a sink constructed directly from the given Core API processor
     * meta-supplier.
     *
     * @param sinkName user-friendly sink name
     * @param metaSupplier the processor meta-supplier
     */
    public static Sink fromProcessor(String sinkName, ProcessorMetaSupplier metaSupplier) {
        return new SinkImpl(sinkName, metaSupplier);
    }

    /**
     * Returns a sink that will put {@code Map.Entry}s it receives into a
     * Hazelcast {@code IMap}.
     */
    public static Sink writeMap(String mapName) {
        return new SinkImpl("writeMap(" + mapName + ')', SinkProcessors.writeMap(mapName));
    }

    /**
     * Returns a sink that will put {@code Map.Entry}s it receives into a
     * Hazelcast {@code IMap} in a remote cluster.
     */
    public static Sink writeMap(String mapName, ClientConfig clientConfig) {
        return new SinkImpl("writeMap(" + mapName + ')', SinkProcessors.writeMap(mapName, clientConfig));
    }

    /**
     * Returns a sink that will put {@code Map.Entry}s it receives into a
     * Hazelcast {@code ICache}.
     */
    public static Sink writeCache(String cacheName) {
        return new SinkImpl("writeCache(" + cacheName + ')', SinkProcessors.writeCache(cacheName));
    }

    /**
     * Returns a sink that will put {@code Map.Entry}s it receives into a
     * Hazelcast {@code ICache} in a remote cluster.
     */
    public static Sink writeCache(String cacheName, ClientConfig clientConfig) {
        return new SinkImpl("writeCache(" + cacheName + ')', SinkProcessors.writeCache(cacheName, clientConfig));
    }

    /**
     * Returns a sink that will write the items it receives to a Hazelcast
     * {@code IList}.
     */
    public static Sink writeList(String listName) {
        return new SinkImpl("writeList(" + listName + ')', SinkProcessors.writeList(listName));
    }

    /**
     * Returns a sink that will write the items it receives to a Hazelcast
     * {@code IList} in a remote cluster.
     */
    public static Sink writeList(String listName, ClientConfig clientConfig) {
        return new SinkImpl("writeList(" + listName + ')', SinkProcessors.writeList(listName, clientConfig));
    }

    /**
     * Returns a sink which connects to the specified TCP socket and writes
     * to it a string representation of the items it receives. It converts
     * an item to its string representation using the supplied
     * {@code toStringF} function.
     */
    public static <T> Sink writeSocket(
            @Nonnull String host,
            int port,
            @Nonnull DistributedFunction<T, String> toStringF,
            @Nonnull Charset charset
    ) {
        return new SinkImpl("writeSocket(" + host + ':' + port + ')',
                SinkProcessors.writeSocket(host, port, toStringF, charset));
    }

    /**
     * Convenience for {@link #writeSocket(String, int, DistributedFunction,
     * Charset)} with UTF-8 as the charset.
     */
    public static <T> Sink writeSocket(
            @Nonnull String host,
            int port,
            @Nonnull DistributedFunction<T, String> toStringF
    ) {
        return new SinkImpl("writeSocket(" + host + ':' + port + ')',
                SinkProcessors.writeSocket(host, port, toStringF, UTF_8));
    }

    /**
     * Convenience for {@link #writeSocket(String, int, DistributedFunction,
     * Charset)} with {@code Object.toString} as the conversion function and
     * UTF-8 as the charset.
     */
    public static <T> Sink writeSocket(@Nonnull String host, int port) {
        return new SinkImpl("writeSocket(" + host + ':' + port + ')',
                SinkProcessors.writeSocket(host, port, Object::toString, UTF_8));
    }
}
