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

package com.hazelcast.jet.pipeline;

/**
 * The basic element of a Jet {@link Pipeline pipeline}.
 * To build a pipeline, start with {@link Pipeline#drawFrom(BatchSource)} to
 * get the initial {@link BatchStage} and then use its methods to attach
 * further downstream stages. Terminate the pipeline by calling {@link
 * BatchStage#drainTo(Sink)}, which will attach a {@link SinkStage}.
 */
public interface Stage {
    /**
     * Returns the {@link Pipeline} this pipeline belongs to.
     */
    Pipeline getPipeline();
}
