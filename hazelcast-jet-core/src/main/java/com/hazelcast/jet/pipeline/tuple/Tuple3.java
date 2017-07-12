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

/**
 * Javadoc pending.
 */
public class Tuple3<E1, E2, E3> {
    private E1 f1;
    private E2 f2;
    private E3 f3;

    public Tuple3(E1 f1, E2 f2, E3 f3) {
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
    }

    public E1 f1() {
        return f1;
    }

    public E2 f2() {
        return f2;
    }

    public E3 f3() {
        return f3;
    }

    @Override
    public boolean equals(Object obj) {
        final Tuple3 that;
        return this == obj
                || obj instanceof Tuple3
                && Objects.equals(this.f1, (that = (Tuple3) obj).f1)
                && Objects.equals(this.f2, that.f2)
                && Objects.equals(this.f3, that.f3);
    }

    @Override
    public int hashCode() {
        int hc = 17;
        hc = 73 * hc + f1.hashCode();
        hc = 73 * hc + f2.hashCode();
        hc = 73 * hc + f3.hashCode();
        return hc;
    }
}
