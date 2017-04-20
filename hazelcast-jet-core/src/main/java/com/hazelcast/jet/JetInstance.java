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

package com.hazelcast.jet;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.stream.IStreamCache;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.jet.stream.IStreamMap;

/**
 * Represents either an instance of a Jet server node or a Jet client
 * instance that connects to a remote cluster.
 */
public interface JetInstance {

    /**
     * Returns the name of the Jet instance.
     */
    String getName();

    /**
     * Returns the underlying Hazelcast IMDG instance used by Jet. It will
     * be either a server node or a client, depending on the type of this
     * {@code JetInstance}.
     */
    HazelcastInstance getHazelcastInstance();

    /**
     * Returns information about the cluster this Jet instance is part of.
     */
    Cluster getCluster();

    /**
     * Returns the configuration for this Jet member. This method is not
     * available on client instances.
     */
    JetConfig getConfig();

    /**
     * Creates and returns an executable job based on a given DAG.
     *
     * @param dag The DAG that will be used to for the execution of the job
     * @return a new {@link Job} instance
     */
    Job newJob(DAG dag);

    /**
     * Creates and returns an executable Job based on the supplied DAG and job
     * configuration.
     *
     * @return a new {@code Job} instance
     */
    Job newJob(DAG dag, JobConfig config);

    /**
     * Returns the distributed map instance with the specified name.
     *
     * @param name name of the distributed map
     * @return distributed map instance with the specified name
     */
    <K, V> IStreamMap<K, V> getMap(String name);

    /**
     * Returns the distributed cache instance with the specified name.
     *
     * @param name name of the distributed cache
     * @return distributed cache instance with the specified name
     */
    <K, V> IStreamCache<K, V> getCache(String name);

    /**
     * Returns the distributed list instance with the specified name.
     * Index-based operations on the list are not supported.
     *
     * @param name name of the distributed list
     * @return distributed list instance with the specified name
     */
    <E> IStreamList<E> getList(String name);

    /**
     * Shutdowns the current instance.
     */
    void shutdown();

}
