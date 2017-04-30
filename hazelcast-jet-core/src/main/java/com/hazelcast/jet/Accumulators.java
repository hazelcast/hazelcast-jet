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

package com.hazelcast.jet;

import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Objects;

/**
 * Mutable holders of primitive values and references with support
 * for some common accumulation operations.
 */
public final class Accumulators {

    private Accumulators() {
    }

    /**
     * Accumulator of a primitive {@code long} value.
     */
    public static class LongAccumulator {

        private long value;

        /**
         * Creates a new instance with {@code value == 0}.
         */
        public LongAccumulator() {
        }

        /**
         * Creates a new instance with the specified value.
         */
        public LongAccumulator(long value) {
            this.value = value;
        }

        /**
         * Returns the current value.
         */
        public long get() {
            return value;
        }

        /**
         * Sets the value as given.
         */
        public LongAccumulator set(long value) {
            this.value = value;
            return this;
        }

        /**
         * Uses {@link Math#addExact(long, long) Math.addExact()} to add the
         * supplied value to this accumulator.
         */
        public LongAccumulator addExact(long value) {
            this.value = Math.addExact(this.value, value);
            return this;
        }

        /**
         * Uses {@link Math#addExact(long, long) Math.addExact()} to add the value
         * of the supplied accumulator into this one.
         */
        public LongAccumulator addExact(LongAccumulator that) {
            this.value = Math.addExact(this.value, that.value);
            return this;
        }

        /**
         * Subtracts the value of the supplied accumulator from this one.
         */
        public LongAccumulator subtract(LongAccumulator that) {
            this.value -= that.value;
            return this;
        }

        /**
         * Uses {@link Math#subtractExact(long, long) Math.subtractExact()}
         * to subtract the value of the supplied accumulator from this one.
         */
        public LongAccumulator subtractExact(LongAccumulator that) {
            this.value = Math.subtractExact(this.value, that.value);
            return this;
        }

        @Override
        public boolean equals(Object o) {
            return this == o ||
                    o != null
                    && this.getClass() == o.getClass()
                    && this.value == ((LongAccumulator) o).value;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(value);
        }

        @Override
        public String toString() {
            return "MutableLong(" + value + ')';
        }
    }

    /**
     * Accumulator of a primitive {@code double} value.
     */
    public static class DoubleAccumulator {

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
            return "MutableDouble(" + value + ')';
        }
    }

    /**
     * Mutable container of an object reference.
     *
     * @param <T> referenced object type
     */
    public static class MutableReference<T> {

        /**
         * The holder's value.
         */
        private T value;

        /**
         * Creates a new instance with a {@code null} value.
         */
        public MutableReference() {
        }

        /**
         * Creates a new instance with the specified value.
         */
        public MutableReference(T value) {
            this.value = value;
        }

        /**
         * Returns the current value.
         */
        public T get() {
            return value;
        }

        /**
         * Sets the value as given.
         */
        public MutableReference set(T value) {
            this.value = value;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            return this == o ||
                    o != null
                    && this.getClass() == o.getClass()
                    && Objects.equals(this.value, ((MutableReference) o).value);
        }

        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "MutableReference(" + value + ')';
        }
    }
}
