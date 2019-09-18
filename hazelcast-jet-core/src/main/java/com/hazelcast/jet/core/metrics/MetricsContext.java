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

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Context object which is able to provide localized counter instances to
 * be used as sources of custom metrics. Each {@link com.hazelcast.jet.core.Processor}
 * will have its own instance. Hence user metrics have the same granularity
 * as Processors.
 */
public interface MetricsContext {

    /**
     * Return a counter that can be used to set values for a custom, per
     * Processor metric.
     */
    @Nonnull
    AtomicLong getCounter(String name);
}
