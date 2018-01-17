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

package com.hazelcast.jet.accumulator;

/**
 * Mutable container of a {@code double} value.
 */
public class DoubleAccumulator {

    private double value;

    /**
     * Creates a new instance with {@code value == 0}.
     */
    public DoubleAccumulator() {
    }

    /**
     * Creates a new instance with the specified value.
     */
    public DoubleAccumulator(double value) {
        this.value = value;
    }

    /**
     * Returns the current value.
     */
    public double get() {
        return value;
    }

    /**
     * Sets the value as given.
     */
    public DoubleAccumulator set(double value) {
        this.value = value;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        return this == o ||
                o != null
                && this.getClass() == o.getClass()
                && this.value == ((DoubleAccumulator) o).value;
    }

    @Override
    public int hashCode() {
        return Double.hashCode(value);
    }

    @Override
    public String toString() {
        return "DoubleAccumulator(" + value + ')';
    }

    /**
     * Adds the value to this objects' value.
     */
    public  DoubleAccumulator add(double v) {
        value += v;
        return this;
    }

    /**
     * Adds the value of the supplied accumulator to this one.
     */
    public DoubleAccumulator add(DoubleAccumulator that) {
        value += that.value;
        return this;
    }

    /**
     * Subtracts the value of the supplied accumulator from this one.
     */
    public DoubleAccumulator subtract(DoubleAccumulator that) {
        value -= that.value;
        return this;
    }
}
