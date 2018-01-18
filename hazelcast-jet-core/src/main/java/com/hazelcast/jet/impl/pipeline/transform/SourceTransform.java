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

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.Source;

import javax.annotation.Nonnull;

import java.util.List;

import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static java.util.Collections.emptyList;

public class SourceTransform<T> implements Source<T>, Transform {
    @Nonnull
    public final ProcessorMetaSupplier metaSupplier;
    @Nonnull
    private final String name;

    public SourceTransform(
            @Nonnull String name,
            @Nonnull ProcessorMetaSupplier metaSupplier
    ) {
        this.metaSupplier = metaSupplier;
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public List<? extends Transform> upstream() {
        return emptyList();
    }

    @Override
    public String toString() {
        return name;
    }
}
