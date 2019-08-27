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

package com.hazelcast.jet;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.jet.config.JetClientConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Base class for tests that share the cluster for all jobs. The subclass must
 * call {@link #beforeClassWithClient} method.
 */
@RunWith(HazelcastSerialClassRunner.class)
public class SimpleTestInClusterSupport extends JetTestSupport {

    private static JetTestInstanceFactory factory;
    private static JetConfig jetConfig;
    private static JetInstance[] instances;
    private static JetInstance client;

    protected static void beforeClass(int memberCount, @Nullable JetConfig jetConfig) {
        factory = new JetTestInstanceFactory();
        instances = new JetInstance[memberCount];
        if (jetConfig == null) {
            jetConfig = new JetConfig();
        }
        SimpleTestInClusterSupport.jetConfig = jetConfig;
        // create members
        for (int i = 0; i < memberCount; i++) {
            instances[i] = factory.newMember(jetConfig);
        }
    }

    protected static void beforeClassWithClient(
            int memberCount,
            @Nullable JetConfig config,
            @Nullable JetClientConfig clientConfig
    ) {
        beforeClass(memberCount, config);

        if (clientConfig == null) {
            clientConfig = new JetClientConfig();
        }
        client = factory.newClient(clientConfig);
    }

    @After
    public void supportAfter() {
        // after each test ditch all jobs and objects
        for (Job job : instances[0].getJobs()) {
            ditchJob(job, instances());
        }
        for (DistributedObject o : instances()[0].getHazelcastInstance().getDistributedObjects()) {
            o.destroy();
        }
    }

    @AfterClass
    public static void tearDown() {
        factory.terminateAll();
        factory = null;
        instances = null;
        client = null;
    }

    @Nonnull
    protected static JetTestInstanceFactory factory() {
        return factory;
    }

    /**
     * Returns the config used to create member instances (even if null was
     * passed).
     */
    @Nonnull
    protected static JetConfig jetConfig() {
        return jetConfig;
    }

    /**
     * Returns the first instance.
     */
    @Nonnull
    protected static JetInstance instance() {
        return instances[0];
    }

    /**
     * Returns all instances (except for the client).
     */
    @Nonnull
    protected static JetInstance[] instances() {
        return instances;
    }

    /**
     * Returns the client or null, if a client wasn't requested.
     */
    protected static JetInstance client() {
        return client;
    }
}
