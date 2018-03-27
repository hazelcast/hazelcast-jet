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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static java.lang.Math.min;
import static java.util.stream.IntStream.rangeClosed;

public final class ReadIListP extends AbstractProcessor {

    static final int FETCH_SIZE = 16384;
    private final String name;
    private final SerializableClientConfig clientConfig;

    private Traverser<Object> traverser;
    private HazelcastInstance client;

    ReadIListP(String name, SerializableClientConfig clientConfig) {
        this.name = name;
        this.clientConfig = clientConfig;
    }

    @Override
    protected void init(@Nonnull Context context) {
        HazelcastInstance instance;
        if (isRemote()) {
            instance = client = newHazelcastClient(clientConfig.asClientConfig());
        } else {
            instance = context.jetInstance().getHazelcastInstance();
        }
        IList<Object> list = instance.getList(name);
        final int size = list.size();
        traverser = size <= FETCH_SIZE ?
                traverseIterable(list)
                :
                traverseStream(rangeClosed(0, size / FETCH_SIZE).mapToObj(chunkIndex -> chunkIndex * FETCH_SIZE))
                        .flatMap(start -> traverseIterable(list.subList(start, min(start + FETCH_SIZE, size))));
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverser);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public void close(@Nullable Throwable error) {
        if (client != null) {
            client.shutdown();
        }
    }

    private boolean isRemote() {
        return clientConfig != null;
    }

    public static ProcessorMetaSupplier metaSupplier(String listName, ClientConfig clientConfig) {
        SerializableClientConfig config = clientConfig != null ? new SerializableClientConfig(clientConfig) : null;
        return forceTotalParallelismOne(
                ProcessorSupplier.of(() -> new ReadIListP(listName, config)), listName
        );
    }
}
