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

package com.hazelcast.jet.pipeline.samples;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.pipeline.ComputeStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;

import java.util.Map.Entry;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.pipeline.Sources.streamKafka;
import static com.hazelcast.jet.pipeline.Transforms.slidingWindow;

public class PipelineStockMarket {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        ComputeStage<Entry<String, Long>> c = p.drawFrom(streamKafka());
        c.attach(slidingWindow(entryKey(), slidingWindowDef(1, 1), counting()))
         .drainTo(Sinks.writeMap("sink"));
        p.execute(Jet.newJetInstance());
    }
}
