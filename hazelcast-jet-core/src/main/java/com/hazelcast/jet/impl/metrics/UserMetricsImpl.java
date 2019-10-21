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

package com.hazelcast.jet.impl.metrics;

import com.hazelcast.jet.core.metrics.Counter;
import com.hazelcast.jet.core.metrics.Gauge;
import com.hazelcast.jet.core.metrics.Unit;
import com.hazelcast.jet.impl.execution.ProcessorTasklet;
import com.hazelcast.jet.impl.execution.Tasklet;

public final class UserMetricsImpl {

    private static final ThreadLocal<Container> CONTEXT = ThreadLocal.withInitial(Container::new);

    private UserMetricsImpl() {
    }

    public static Container container() {
        return CONTEXT.get();
    }

    public static Counter counter(String name) {
        return getContext().counter(name);
    }

    public static Gauge gauge(String name, Unit unit) {
        return getContext().gauge(name, unit);
    }

    private static UserMetricsContext getContext() {
        Container container = CONTEXT.get();
        UserMetricsContext context = container.getContext();
        if (context == null) {
            throw new RuntimeException("User metrics are accessible only from internal worker threads");
        }
        return context;
    }

    public static class Container {

        private UserMetricsContext context;

        Container() {
        }

        public UserMetricsContext getContext() {
            return context;
        }

        public void setContext(Tasklet tasklet) {
            if (tasklet instanceof ProcessorTasklet) {
                this.context = ((ProcessorTasklet) tasklet).getUserMetricsContext();
            }
        }
    }

}
