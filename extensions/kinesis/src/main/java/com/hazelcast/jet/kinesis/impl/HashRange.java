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
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Objects;

import static java.math.BigInteger.ZERO;
import static java.math.BigInteger.valueOf;

public class HashRange implements Serializable { //todo: is it worth to use better serialization?

    private final BigInteger minInclusive;
    private final BigInteger maxExclusive;

    public HashRange(long minInclusive, long maxExclusive) {
        this(BigInteger.valueOf(minInclusive), BigInteger.valueOf(maxExclusive));
    }

    public HashRange(@Nonnull BigInteger minInclusive, @Nonnull BigInteger maxExclusive) {
        if (minInclusive.compareTo(ZERO) < 0) {
            throw new IllegalArgumentException("Partition start can't be negative");
        }
        if (maxExclusive.compareTo(ZERO) <= 0) {
            throw new IllegalArgumentException("Partition end can't be negative or zero");
        }
        if (maxExclusive.compareTo(minInclusive) <= 0) {
            throw new IllegalArgumentException("Partition end can't be smaller or equal to partition start");
        }
        this.minInclusive = Objects.requireNonNull(minInclusive, "minInclusive");
        this.maxExclusive = Objects.requireNonNull(maxExclusive, "maxExclusive");
    }

    public HashRange partition(int index, int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("Count must be a strictly positive value");
        }
        if (index < 0 || index >= count) {
            throw new IllegalArgumentException("Index must be between 0 and " + count);
        }
        BigInteger partitionSize = size().divide(valueOf(count));
        BigInteger partitionStart = minInclusive.add(partitionSize.multiply(valueOf(index)));
        BigInteger partitionEnd = partitionStart.add(partitionSize);
        return new HashRange(partitionStart, partitionEnd);
    }

    private BigInteger size() {
        return maxExclusive.subtract(minInclusive);
    }

    public boolean contains(String stringValue) {
        BigInteger value = new BigInteger(stringValue);
        return value.compareTo(minInclusive) >= 0 && value.compareTo(maxExclusive) < 0;
    }

    @Override
    public int hashCode() {
        return minInclusive.hashCode() + 31 * maxExclusive.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        HashRange range = (HashRange) obj;
        return minInclusive.equals(range.minInclusive) && maxExclusive.equals(range.maxExclusive);
    }

    @Override
    public String toString() {
        return "HashRange[" + minInclusive + ", " + maxExclusive + ")";
    }

}
