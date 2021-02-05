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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class AvgSqlAggregationTest {

    @SuppressWarnings("unused")
    private Object[] types() {
        return new Object[]{
                QueryDataType.DECIMAL,
                QueryDataType.DOUBLE
        };
    }

    @Test
    @Parameters(method = "types")
    public void test_default(QueryDataType operandType) {
        AvgSqlAggregation aggregation = new AvgSqlAggregation(0, operandType);

        assertThat(aggregation.collect()).isNull();
    }

    @SuppressWarnings("unused")
    private Object[] values() {
        return new Object[]{
                new Object[]{QueryDataType.DECIMAL, new BigDecimal(1), new BigDecimal(2),
                        new BigDecimal("1.5")},
                new Object[]{QueryDataType.DECIMAL, new BigDecimal("9223372036854775808998"),
                        new BigDecimal("9223372036854775808999"), new BigDecimal("9223372036854775808998.5")},
                new Object[]{QueryDataType.DOUBLE, 1D, 2D, 1.5D}
        };
    }

    @Test
    @Parameters(method = "values")
    public void test_accumulate(QueryDataType operandType, Object value1, Object value2, Object expected) {
        AvgSqlAggregation aggregation = new AvgSqlAggregation(0, operandType);
        aggregation.accumulate(new Object[]{value1});
        aggregation.accumulate(new Object[]{value2});

        assertThat(aggregation.collect()).isEqualTo(expected);
    }

    @Test
    public void test_periodicDecimal() {
        AvgSqlAggregation aggregation = new AvgSqlAggregation(0, QueryDataType.DECIMAL);
        aggregation.accumulate(new Object[]{BigDecimal.ZERO});
        aggregation.accumulate(new Object[]{BigDecimal.ONE});
        aggregation.accumulate(new Object[]{BigDecimal.ONE});

        assertThat(aggregation.collect()).isEqualTo(new BigDecimal("0.66666666666666666666666666666666666667"));
    }

    @Test
    public void test_accumulateDistinct() {
        AvgSqlAggregation aggregation = new AvgSqlAggregation(0, QueryDataType.DOUBLE, true);
        aggregation.accumulate(new Object[]{null});
        aggregation.accumulate(new Object[]{1.0});
        aggregation.accumulate(new Object[]{1.0});
        aggregation.accumulate(new Object[]{2.0});

        assertThat(aggregation.collect()).isEqualTo(1.5);
    }

    @Test
    @Parameters(method = "values")
    public void test_combine(QueryDataType operandType, Object value1, Object value2, Object expected) {
        AvgSqlAggregation left = new AvgSqlAggregation(0, operandType);
        left.accumulate(new Object[]{value1});

        AvgSqlAggregation right = new AvgSqlAggregation(0, operandType);
        right.accumulate(new Object[]{value2});

        left.combine(right);

        assertThat(left.collect()).isEqualTo(expected);
    }

    @Test
    public void test_serialization() {
        AvgSqlAggregation original = new AvgSqlAggregation(0, QueryDataType.DOUBLE);
        original.accumulate(new Object[]{1.0});

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        AvgSqlAggregation serialized = ss.toObject(ss.toData(original));

        assertThat(serialized).isEqualToComparingFieldByField(original);
    }
}
