/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream.impl.collectors;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.jet2.ProcessorMetaSupplier;
import com.hazelcast.jet2.Processors;

import static com.hazelcast.jet.stream.impl.StreamUtil.LIST_PREFIX;
import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;

public class HazelcastListCollector<T> extends AbstractHazelcastCollector<T, IList<T>> {

    private final String listName;

    public HazelcastListCollector() {
        this(randomName(LIST_PREFIX));
    }

    public HazelcastListCollector(String listName) {
        this.listName = listName;
    }

    @Override
    protected IList<T> getTarget(HazelcastInstance instance) {
        return instance.getList(listName);
    }

    @Override
    protected ProcessorMetaSupplier getConsumer() {
        return ProcessorMetaSupplier.of(Processors.listWriter(listName));
    }

    @Override
    protected int parallelism() {
        return 1;
    }

    @Override
    protected String getName() {
        return listName;
    }

}
