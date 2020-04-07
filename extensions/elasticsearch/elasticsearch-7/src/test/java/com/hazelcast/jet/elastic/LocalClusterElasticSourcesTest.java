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

package com.hazelcast.jet.elastic;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.test.SerialTest;
import org.junit.experimental.categories.Category;

import java.util.function.Supplier;

/**
 * Test running 3 local Jet members in a cluster and Elastic in docker
 */
@Category(SerialTest.class)
public class LocalClusterElasticSourcesTest extends CommonElasticSourcesTest {

    // Cluster startup takes >1s, reusing the cluster between tests
    private static Supplier<JetInstance> jet = Util.memoize(() -> {
        JetTestInstanceFactory factory = new JetTestInstanceFactory();
        JetInstance[] instances = factory.newMembers(new JetConfig(), 3);
        Runtime.getRuntime().addShutdownHook(new Thread(factory::terminateAll));
        return instances[0];
    });

    @Override
    protected JetInstance createJetInstance() {
        return jet.get();
    }
}
