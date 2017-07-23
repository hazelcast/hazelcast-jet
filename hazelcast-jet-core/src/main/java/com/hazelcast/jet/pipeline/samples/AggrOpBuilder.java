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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Javadoc pending.
 */
@SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
public class AggrOpBuilder {
    void build1() {
        AggregateOperation2<Integer, String, Holder2<Integer, String>, String> aggrOp = AggregateOperation
                .withCreate(Holder2<Integer, String>::new)
                .<Integer>andAccumulate0(Holder2::take1)
                .<String>andAccumulate1(Holder2::take2)
                .andCombine(Holder2::combine)
                .andFinish(Object::toString);
    }

    static class Holder2<T1, T2> {
        void take1(T1 item) {
        }
        void take2(T2 item) {
        }
        void combine(Holder2<T1, T2> that) {
        }
    }
}
