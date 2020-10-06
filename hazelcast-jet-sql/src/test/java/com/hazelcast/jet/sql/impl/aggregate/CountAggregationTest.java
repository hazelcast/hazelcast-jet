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
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CountAggregationTest {

    @Test
    public void test_default() {
        CountAggregation aggregation = new CountAggregation();

        assertThat(aggregation.collect()).isEqualTo(0L);
    }

    @Test
    public void test_accumulate() {
        CountAggregation aggregation = new CountAggregation();
        aggregation.accumulate(new Object[0]);
        aggregation.accumulate(new Object[0]);

        assertThat(aggregation.collect()).isEqualTo(2L);
    }

    @Test
    public void test_combine() {
        CountAggregation left = new CountAggregation();
        left.accumulate(new Object[0]);

        CountAggregation right = new CountAggregation();
        right.accumulate(new Object[0]);

        left.combine(right);

        assertThat(left.collect()).isEqualTo(2L);
        assertThat(right.collect()).isEqualTo(1L);
    }

    @Test
    public void test_serialization() {
        CountAggregation original = new CountAggregation();
        original.accumulate(new Object[0]);

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        CountAggregation serialized = ss.toObject(ss.toData(original));

        assertThat(serialized).isEqualTo(original);
    }
}
