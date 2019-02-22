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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.pipeline.WindowDefinition;

public abstract class WindowDefinitionBase implements WindowDefinition {
    private long earlyResultPeriod;

    /**
     * Returns the optimal watermark stride for this window definition.
     * Watermarks that are more spaced out are better for performance, but they
     * hurt the responsiveness of a windowed pipeline stage. The Planner will
     * determine the actual stride, which may be an integer fraction of the
     * value returned here.
     */
    public abstract long preferredWatermarkStride();

    public long earlyResultsPeriod() {
        return earlyResultPeriod;
    }

    @Override
    public WindowDefinition setEarlyResultsPeriod(long earlyResultPeriod) {
        this.earlyResultPeriod = earlyResultPeriod;
        return this;
    }

    public abstract String name();
}
