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
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Objects;

import static com.hazelcast.sql.impl.expression.math.ExpressionMath.DECIMAL_MATH_CONTEXT;

@NotThreadSafe
class AvgAggregation implements Aggregation {

    private int index;
    private QueryDataType resultType;

    private SumAggregation sum;
    private CountAggregation count;

    @SuppressWarnings("unused")
    private AvgAggregation() {
    }

    AvgAggregation(int index, QueryDataType operandType) {
        this.index = index;
        this.resultType = inferResultType(operandType);

        this.sum = new SumAggregation(index, operandType);
        this.count = new CountAggregation();
    }

    private static QueryDataType inferResultType(QueryDataType operandType) {
        switch (operandType.getTypeFamily()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
                return QueryDataType.DECIMAL;
            case REAL:
            case DOUBLE:
                return QueryDataType.DOUBLE;
            default:
                throw QueryException.error("Unsupported operand type: " + operandType);
        }
    }

    @Override
    public QueryDataType resultType() {
        return resultType;
    }

    @Override
    public void accumulate(Object[] row) {
        Object value = row[index];
        if (value != null) {
            sum.accumulate(row);
            count.accumulate(row);
        }
    }

    @Override
    public void combine(Aggregation other0) {
        AvgAggregation other = (AvgAggregation) other0;

        sum.combine(other.sum);
        count.combine(other.count);
    }

    @Override
    public Object collect() {
        Object sum0 = this.sum.collect();
        if (sum0 == null) {
            return null;
        }
        Object count0 = this.count.collect();

        switch (resultType.getTypeFamily()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
                BigDecimal decimalSum = this.sum.resultType().getConverter().asDecimal(sum0);
                BigDecimal decimalCount = this.count.resultType().getConverter().asDecimal(count0);
                return decimalSum.divide(decimalCount, DECIMAL_MATH_CONTEXT);
            default:
                assert resultType.getTypeFamily() == QueryDataTypeFamily.DOUBLE;
                double doubleSum = this.sum.resultType().getConverter().asDouble(sum0);
                double doubleCount = this.count.resultType().getConverter().asDouble(count0);
                return doubleSum / doubleCount;
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(index);
        out.writeObject(resultType);
        out.writeObject(sum);
        out.writeObject(count);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        index = in.readInt();
        resultType = in.readObject();
        sum = in.readObject();
        count = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AvgAggregation that = (AvgAggregation) o;
        return index == that.index &&
                Objects.equals(resultType, that.resultType) &&
                Objects.equals(sum, that.sum) &&
                Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, resultType, sum, count);
    }
}
