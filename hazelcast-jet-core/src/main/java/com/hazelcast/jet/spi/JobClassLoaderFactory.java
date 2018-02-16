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

package com.hazelcast.jet.spi;

import java.util.Map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;

/**
 * A SPI interface that can be implemented to customize the instantiation of a job's class-loader.
 * <p>
 * This factory is instantiated using the class-loader set in Hazelcast config.
 * <p>
 * This is primarily useful in OSGi run-times.
 */
public interface JobClassLoaderFactory {

    /**
     * Returns the job's ClassLoader.
     *
     * @param jobId
     *            the job identifier
     * @param hi
     *            the {@link HazelcastInstance}
     * @param resources
     *            the {@link JobConfig} resources
     * @return the jobId's ClassLoader
     */
    ClassLoader getJobClassLoader(long jobId, HazelcastInstance hi, Map<String, byte[]> resources);

}
