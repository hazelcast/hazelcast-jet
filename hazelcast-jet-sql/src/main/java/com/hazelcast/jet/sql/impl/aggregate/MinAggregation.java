/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.Objects;

@NotThreadSafe
class MinAggregation implements Aggregation {

    private int index;
    private QueryDataType operandType;

    private Object value;

    @SuppressWarnings("unused")
    private MinAggregation() {
    }

    MinAggregation(int index, QueryDataType operandType) {
        this.index = index;
        this.operandType = operandType;
    }

    @Override
    public QueryDataType resultType() {
        return operandType;
    }

    @Override
    public void accumulate(Object[] row) {
        Object value = row[index];

        if (this.value == null || (value != null && compare(this.value, value) < 0)) {
            this.value = value;
        }
    }

    @Override
    public void combine(Aggregation other0) {
        MinAggregation other = (MinAggregation) other0;

        Object value = other.value;

        if (this.value == null || (value != null && compare(this.value, value) < 0)) {
            this.value = value;
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private int compare(Object left, Object right) {
        assert left != null;
        assert right != null;

        Comparable leftComparable = asComparable(left);
        Comparable rightComparable = asComparable(right);

        return rightComparable.compareTo(leftComparable);
    }

    private static Comparable<?> asComparable(Object value) {
        if (value instanceof Comparable) {
            return (Comparable<?>) value;
        } else {
            throw QueryException.error("Value is not comparable: " + value);
        }
    }

    @Override
    public Object collect() {
        return value;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(index);
        out.writeObject(operandType);
        out.writeObject(value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        index = in.readInt();
        operandType = in.readObject();
        value = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MinAggregation that = (MinAggregation) o;
        return index == that.index &&
                Objects.equals(operandType, that.operandType) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, operandType, value);
    }
}
