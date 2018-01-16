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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.impl.pipeline.transform.SourceTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.SourceWithTimestamp;

/**
 * Javadoc pending.
 */
public class SourceWithTimestampImpl<T> implements SourceWithTimestamp<T>, Transform {

    private final SourceTransform<T> source;
    private final WatermarkGenerationParams wmGenParams;

    public SourceWithTimestampImpl(SourceTransform<T> source, WatermarkGenerationParams wmGenParams) {
        this.source = source;
        this.wmGenParams = wmGenParams;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public SourceTransform<T> source() {
        return source;
    }

    public WatermarkGenerationParams wmGenParams() {
        return wmGenParams;
    }
}
