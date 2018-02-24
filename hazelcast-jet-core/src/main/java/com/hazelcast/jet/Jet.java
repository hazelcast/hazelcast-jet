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

package com.hazelcast.jet;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.JetInstanceImpl;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.config.XmlJetConfigBuilder;
import com.hazelcast.map.merge.IgnoreMergingEntryMapMergePolicy;

import static com.hazelcast.jet.impl.config.XmlJetConfigBuilder.getClientConfig;

/**
 * Entry point to the Jet product.
 */
public final class Jet {

    /**
     * Prefix of all Hazelcast internal objects used by Jet (such as job
     * metadata, snapshots etc.)
     */
    public static final String INTERNAL_JET_OBJECTS_PREFIX = "__jet.";

    private Jet() {
    }

    /**
     * Creates a member of the Jet cluster with the given configuration.
     */
    public static JetInstance newJetInstance(JetConfig config) {
        configureJetService(config);
        HazelcastInstanceImpl hazelcastInstance = ((HazelcastInstanceProxy)
                Hazelcast.newHazelcastInstance(config.getHazelcastConfig())).getOriginal();
        return new JetInstanceImpl(hazelcastInstance, config);
    }

    /**
     * Creates a member of the Jet cluster with the default configuration.
     */
    public static JetInstance newJetInstance() {
        JetConfig config = XmlJetConfigBuilder.getConfig();
        return newJetInstance(config);
    }

    /**
     * Creates a Jet client with the default configuration.
     */
    public static JetInstance newJetClient() {
        ClientConfig clientConfig = getClientConfig();
        return newJetClient(clientConfig);
    }

    /**
     * Creates a Jet client with the given Hazelcast client configuration.
     */
    public static JetInstance newJetClient(ClientConfig config) {
        return getJetClientInstance(HazelcastClient.newHazelcastClient(config));
    }

    /**
     * Shuts down all running Jet client and member instances.
     */
    public static void shutdownAll() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    static JetClientInstanceImpl getJetClientInstance(HazelcastInstance client) {
        return new JetClientInstanceImpl(((HazelcastClientProxy) client).client);
    }

    static void configureJetService(JetConfig jetConfig) {
        if (!(jetConfig.getHazelcastConfig().getConfigPatternMatcher() instanceof MatchingPointConfigPatternMatcher)) {
            throw new UnsupportedOperationException("Custom config pattern matcher is not supported in Jet");
        }

        jetConfig.getHazelcastConfig().getServicesConfig()
                 .addServiceConfig(new ServiceConfig().setEnabled(true)
                                                      .setName(JetService.SERVICE_NAME)
                                                      .setClassName(JetService.class.getName())
                                                      .setConfigObject(jetConfig));

        jetConfig.getHazelcastConfig().addMapConfig(new MapConfig(INTERNAL_JET_OBJECTS_PREFIX + "*")
                 .setBackupCount(jetConfig.getInstanceConfig().getBackupCount())
                 .setStatisticsEnabled(false)
                 .setMergePolicy(IgnoreMergingEntryMapMergePolicy.class.getName()));
    }
}
