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
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ValueAggregationTest {

    @Test
    public void test_default() {
        ValueAggregation aggregation = new ValueAggregation(0, QueryDataType.OBJECT);

        assertThat(aggregation.collect()).isNull();
    }

    @Test
    public void test_accumulate() {
        ValueAggregation aggregation = new ValueAggregation(1, QueryDataType.VARCHAR);
        aggregation.accumulate(new Object[]{1, "v"});
        aggregation.accumulate(new Object[]{2, "v"});

        assertThat(aggregation.collect()).isEqualTo("v");
    }

    @Test
    public void test_combine() {
        ValueAggregation left = new ValueAggregation(0, QueryDataType.VARCHAR);
        left.accumulate(new Object[]{null});

        ValueAggregation right = new ValueAggregation(0, QueryDataType.VARCHAR);
        right.accumulate(new Object[]{"v"});

        left.combine(right);

        assertThat(left.collect()).isEqualTo("v");
        assertThat(right.collect()).isEqualTo("v");
    }

    @Test
    public void test_serialization() {
        ValueAggregation original = new ValueAggregation(0, QueryDataType.VARCHAR);
        original.accumulate(new Object[]{"v"});

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        ValueAggregation serialized = ss.toObject(ss.toData(original));

        // then
        assertThat(serialized).isEqualTo(original);
    }
}
