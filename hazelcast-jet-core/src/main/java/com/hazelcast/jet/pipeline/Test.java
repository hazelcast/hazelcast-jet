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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;

import static com.hazelcast.jet.Util.entry;

public class Test {

    public static void main(String[] args) {
        JetInstance jet = Jet.newJetInstance();
        Pipeline<Void> pipe = Pipeline.create();

        pipe.apply(Sources.<String, String>readMap("map"))
            .apply(Transforms.map(e -> entry(e.getKey(), e.getValue().toLowerCase())))
            .apply(Sinks.writeMap("sink"));

//        pipe.execute(jet);

    }
}
