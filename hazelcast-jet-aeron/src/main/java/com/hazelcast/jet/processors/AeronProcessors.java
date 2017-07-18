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

package com.hazelcast.jet.processors;

import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.connector.aeron.StreamAeronP;
import com.hazelcast.jet.impl.connector.aeron.WriteAeronP;

import java.util.Set;

import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;

/**
 * Static utility class with factories of Aeron source and sink processors.
 */
public final class AeronProcessors {

    private static final int DEFAULT_FRAGMENT_LIMIT = 100;

    private AeronProcessors() {
    }


    /**
     * @param directoryName
     * @param channel
     * @param streamIds
     * @return
     */
    public static ProcessorMetaSupplier streamAeron(String directoryName,
                                                    String channel,
                                                    Set<Integer> streamIds) {
        return streamAeron(directoryName, channel, streamIds, wholeItem(), DEFAULT_FRAGMENT_LIMIT);
    }

    /**
     * @param directoryName
     * @param channel
     * @param streamIds
     * @param mapperF
     * @param fragmentLimit
     * @param <T>
     * @return
     */
    public static <T> ProcessorMetaSupplier streamAeron(String directoryName,
                                                        String channel,
                                                        Set<Integer> streamIds,
                                                        DistributedFunction<byte[], T> mapperF,
                                                        int fragmentLimit) {
        return new StreamAeronP.MetaSupplier<>(directoryName, channel, streamIds, mapperF, fragmentLimit);
    }

    /**
     * @param directoryName
     * @param channel
     * @param streamId
     * @return
     */
    public static ProcessorSupplier writeAeron(String directoryName,
                                               String channel,
                                               int streamId) {
        return writeAeron(directoryName, channel, streamId, wholeItem());
    }

    /**
     * @param directoryName
     * @param channel
     * @param streamId
     * @param mapperF
     * @param <T>
     * @return
     */
    public static <T> ProcessorSupplier writeAeron(String directoryName,
                                                   String channel,
                                                   int streamId,
                                                   DistributedFunction<T, byte[]> mapperF) {
        return new WriteAeronP.Supplier<>(directoryName, channel, streamId, mapperF);
    }

}
