/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.contrib.elasticsearch;

import com.hazelcast.jet.impl.util.Util;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.function.Supplier;

public class ElasticSupport {

    // Elastic container takes long time to start up, reusing the container for speedup
    public static final Supplier<ElasticsearchContainer> elastic = Util.memoize(() -> {
        ElasticsearchContainer elastic = new ElasticsearchContainer("elasticsearch:7.6.1");
        elastic.start();
        Runtime.getRuntime().addShutdownHook(new Thread(elastic::stop));
        return elastic;
    });

}
