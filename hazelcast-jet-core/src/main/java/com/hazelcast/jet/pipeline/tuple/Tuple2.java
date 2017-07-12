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

package com.hazelcast.jet.pipeline.tuple;

import java.util.Objects;

public class Tuple2<E1, E2> {
    private E1 f1;
    private E2 f2;

    public Tuple2(E1 f1, E2 f2) {
        this.f1 = f1;
        this.f2 = f2;
    }

    public E1 f1() {
        return f1;
    }

    public E2 f2() {
        return f2;
    }

    @Override
    public boolean equals(Object obj) {
        final Tuple2 that;
        return this == obj
                || obj instanceof Tuple2
                && Objects.equals(this.f1, (that = (Tuple2) obj).f1)
                && Objects.equals(this.f2, that.f2);
    }

    @Override
    public int hashCode() {
        int hc = 17;
        hc = 73 * hc + f1.hashCode();
        hc = 73 * hc + f2.hashCode();
        return hc;
    }
}
