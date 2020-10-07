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
public class SumAggregationTest {

    @SuppressWarnings("unused")
    private Object[] types() {
        return new Object[]{
                new Object[]{QueryDataType.TINYINT},
                new Object[]{QueryDataType.SMALLINT},
                new Object[]{QueryDataType.INT},
                new Object[]{QueryDataType.BIGINT},
                new Object[]{QueryDataType.DECIMAL},
                new Object[]{QueryDataType.REAL},
                new Object[]{QueryDataType.DOUBLE}
        };
    }

    @Test
    @Parameters(method = "types")
    public void test_default(QueryDataType operandType) {
        SumAggregation aggregation = new SumAggregation(0, operandType);

        assertThat(aggregation.collect()).isNull();
    }

    @SuppressWarnings("unused")
    private Object[] values() {
        return new Object[]{
                new Object[]{QueryDataType.TINYINT, (byte) 1, (byte) 2, 3L},
                new Object[]{QueryDataType.SMALLINT, (short) 1, (short) 2, 3L},
                new Object[]{QueryDataType.INT, 1, 2, 3L},
                new Object[]{QueryDataType.BIGINT, 1L, 2L, 3L},
                new Object[]{QueryDataType.DECIMAL, new BigDecimal(1), new BigDecimal(2),
                        new BigDecimal(3)},
                new Object[]{QueryDataType.REAL, 1F, 2F, 3D},
                new Object[]{QueryDataType.DOUBLE, 1D, 2D, 3D},
                new Object[]{QueryDataType.TINYINT, (byte) 1, null, 1L},
                new Object[]{QueryDataType.TINYINT, null, (byte) 1, 1L},
                new Object[]{QueryDataType.TINYINT, null, null, null}
        };
    }

    @Test
    @Parameters(method = "values")
    public void test_accumulate(QueryDataType operandType, Object value1, Object value2, Object expected) {
        SumAggregation aggregation = new SumAggregation(0, operandType);
        aggregation.accumulate(new Object[]{value1});
        aggregation.accumulate(new Object[]{value2});

        assertThat(aggregation.collect()).isEqualTo(expected);
    }

    @Test
    @Parameters(method = "values")
    public void test_combine(QueryDataType operandType, Object value1, Object value2, Object expected) {
        SumAggregation left = new SumAggregation(0, operandType);
        left.accumulate(new Object[]{value1});

        SumAggregation right = new SumAggregation(0, operandType);
        right.accumulate(new Object[]{value2});

        left.combine(right);

        assertThat(left.collect()).isEqualTo(expected);
    }

    @Test
    public void test_serialization() {
        SumAggregation original = new SumAggregation(0, QueryDataType.TINYINT);
        original.accumulate(new Object[]{(byte) 1});

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        SumAggregation serialized = ss.toObject(ss.toData(original));

        assertThat(serialized).isEqualTo(original);
    }
}
