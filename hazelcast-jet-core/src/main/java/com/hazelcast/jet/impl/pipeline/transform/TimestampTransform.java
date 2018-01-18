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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.jet.core.WatermarkGenerationParams;

import javax.annotation.Nonnull;

/**
 * Javadoc pending.
 */
public class TimestampTransform<T> extends AbstractTransform implements Transform {
    @Nonnull
    public final WatermarkGenerationParams wmGenParams;

    public TimestampTransform(
            @Nonnull Transform upstream,
            @Nonnull WatermarkGenerationParams wmGenParams
    ) {
        super("timestamp", true, upstream);
        this.wmGenParams = wmGenParams;
    }
}
