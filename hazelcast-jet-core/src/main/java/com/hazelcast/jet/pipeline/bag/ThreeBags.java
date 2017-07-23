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

package com.hazelcast.jet.pipeline.bag;

import java.util.ArrayList;
import java.util.List;

/**
 * Javadoc pending.
 */
public class ThreeBags<E1, E2, E3> {
    private final List<E1> bag1;
    private final List<E2> bag2;
    private final List<E3> bag3;

    public ThreeBags() {
        this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    public ThreeBags(List<E1> bag1, List<E2> bag2, List<E3> bag3) {
        this.bag1 = bag1;
        this.bag2 = bag2;
        this.bag3 = bag3;
    }

    public List<E1> bag1() {
        return bag1;
    }

    public List<E2> bag2() {
        return bag2;
    }

    public List<E3> bag3() {
        return bag3;
    }

    public void combineWith(ThreeBags<E1, E2, E3> that) {
        bag1.addAll(that.bag1());
        bag2.addAll(that.bag2());
        bag3.addAll(that.bag3());
    }
}
