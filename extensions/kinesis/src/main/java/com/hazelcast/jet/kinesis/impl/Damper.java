/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.kinesis.impl;

import javax.annotation.Nonnull;

/**
 * TODO: javadoc, describe what it does
 */
class Damper<T> {

    private final int k;
    private final T[] outputs;
    private final int n;

    private int state; //[0 .. k*n]

    Damper(int k, T[] outputs) {
        if (k < 1) {
            throw new IllegalArgumentException(); //todo
        }
        this.k = k;

        if (outputs == null || outputs.length < 2) {
            throw new IllegalArgumentException(); //todo
        }
        for (T output : outputs) {
            if (output == null) {
                throw new IllegalArgumentException(); //todo
            }
        }
        this.outputs = outputs;
        this.n = outputs.length - 1;
    }

    void ok() {
        if (state > 0) {
            state--;
        }
    }

    void error() {
        if (state <= k * (n - 1)) {
            state += k;
        } else {
            state = k * n;
        }
    }

    @Nonnull
    T output() {
        return outputs[state % k == 0 ? state / k : 1 + state / k];
    }

}
