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
import com.hazelcast.core.IMap;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.map.EntryProcessor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class EntryProcessorWriterP<T, K, V> extends AsyncHazelcastWriterP {

    private final IMap<? super K, ? extends V> map;
    private final FunctionEx<? super T, ? extends K> toKeyFn;
    private final FunctionEx<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn;

    private EntryProcessorWriterP(
        @Nonnull HazelcastInstance instance,
        boolean isLocal,
        @Nonnull String name,
        @Nonnull FunctionEx<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn,
        @Nonnull FunctionEx<? super T, ? extends K> toKeyFn
    ) {
        super(instance, isLocal);
        this.map = instance.getMap(name);
        this.toKeyFn = toKeyFn;
        this.toEntryProcessorFn = toEntryProcessorFn;
    }

    @Override
    protected void processInternal(Inbox inbox) {
        for (Object object; (object = inbox.peek()) != null; ) {
            if (!acquirePermit()) {
                return;
            }
            @SuppressWarnings("unchecked")
            T item = (T) object;
            EntryProcessor<K, V> entryProcessor = toEntryProcessorFn.apply(item);
            K key = toKeyFn.apply(item);
            setCallback(map.submitToKey(key, entryProcessor));
            inbox.remove();
        }
    }

    static final class Supplier<T, K, V> extends AbstractHazelcastConnectorSupplier {

        static final long serialVersionUID = 1L;

        private final String name;
        private final FunctionEx<? super T, ? extends K> toKeyFn;
        private final FunctionEx<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn;

        Supplier(
            @Nonnull String name,
            @Nullable String clientXml,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn
        ) {
            super(clientXml);
            this.name = name;
            this.toKeyFn = toKeyFn;
            this.toEntryProcessorFn = toEntryProcessorFn;
        }

        @Override
        protected Processor createProcessor(HazelcastInstance instance, boolean isLocal) {
            return new EntryProcessorWriterP<>(instance, isLocal, name, toEntryProcessorFn, toKeyFn);
        }
    }

}
