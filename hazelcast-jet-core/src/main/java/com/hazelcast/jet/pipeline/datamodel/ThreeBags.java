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

package com.hazelcast.jet.pipeline.datamodel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Javadoc pending.
 */
public class ThreeBags<E0, E1, E2> implements Serializable {
    private final Collection<E0> bag0;
    private final Collection<E1> bag1;
    private final Collection<E2> bag2;

    public ThreeBags() {
        this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    public ThreeBags(List<E0> bag0, List<E1> bag1, List<E2> bag2) {
        this.bag0 = bag0;
        this.bag1 = bag1;
        this.bag2 = bag2;
    }

    public Collection<E0> bag0() {
        return bag0;
    }

    public Collection<E1> bag1() {
        return bag1;
    }

    public Collection<E2> bag2() {
        return bag2;
    }

    public void combineWith(ThreeBags<E0, E1, E2> that) {
        bag0.addAll(that.bag0());
        bag1.addAll(that.bag1());
        bag2.addAll(that.bag2());
    }

    @Override
    public String toString() {
        return "ThreeBags{" + bag0 + ", " + bag1 + ", " + bag2 + '}';
    }
}
