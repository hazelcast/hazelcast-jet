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
import com.hazelcast.jet.pipeline.StreamSource;

import javax.annotation.Nonnull;

import static java.util.Collections.emptyList;

/**
 * Javadoc pending.
 */
public class StreamSourceTransform<T> extends AbstractTransform implements StreamSource<T> {

    public final boolean emitsJetEvents;

    @Nonnull
    public final ProcessorMetaSupplier metaSupplier;

    public StreamSourceTransform(
            @Nonnull String name,
            boolean emitsJetEvents,
            @Nonnull ProcessorMetaSupplier metaSupplier
    ) {
        super(name, emptyList());
        this.emitsJetEvents = emitsJetEvents;
        this.metaSupplier = metaSupplier;
    }
}
