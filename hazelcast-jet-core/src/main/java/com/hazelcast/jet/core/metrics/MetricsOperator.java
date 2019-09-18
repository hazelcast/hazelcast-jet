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

package com.hazelcast.jet.core.metrics;

/**
 * Marker interface to be implemented by all classes that want to be able
 * to retrieve and work with custom metrics.
 */
public interface MetricsOperator {

    /**
     * Will be called once for each instance and has the role to provide
     * access to the {@link MetricsContext} which can provide the concrete
     * metric counters to use.
     */
    void init(MetricsContext context);

}
