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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.stream.Stream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.impl.util.ImdgUtil.asClientConfig;
import static java.util.stream.Collectors.toList;

public abstract class AbstractHazelcastConnectorSupplier implements ProcessorSupplier {

    private final String clientXml;

    private transient HazelcastInstance instance;

    AbstractHazelcastConnectorSupplier(@Nullable String clientXml) {
        this.clientXml = clientXml;
    }

    @Override
    public void init(@Nonnull Context context) {
        if (clientXml != null) {
            instance = newHazelcastClient(asClientConfig(clientXml));
        } else {
            instance = context.jetInstance().getHazelcastInstance();
        }
    }

    @Nonnull @Override
    public Collection<? extends Processor> get(int count) {
        return Stream.generate(() -> createProcessor(instance))
                     .limit(count)
                     .collect(toList());
    }

    protected abstract Processor createProcessor(HazelcastInstance instance);

    boolean isLocal() {
        return clientXml == null;
    }

    @Override
    public void close(@Nullable Throwable error) {
        if (!isLocal() && instance != null) {
            instance.shutdown();
        }
    }
}
