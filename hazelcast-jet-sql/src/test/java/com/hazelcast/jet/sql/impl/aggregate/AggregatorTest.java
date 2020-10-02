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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class AggregatorTest {

    private Aggregation[] aggregations;

    @Before
    public void setUp() {
        aggregations = new Aggregation[]{
                mock(Aggregation.class),
                mock(Aggregation.class),
                mock(Aggregation.class),
                mock(Aggregation.class)
        };
    }

    @Test
    public void test_accumulate() {
        Aggregator aggregator = new Aggregator(new Aggregation[]{aggregations[0], aggregations[1]});
        Object[] row = new Object[]{1};

        aggregator.accumulate(row);

        verify(aggregations[0]).accumulate(row);
        verify(aggregations[1]).accumulate(row);
    }

    @Test
    public void test_combine() {
        Aggregator left = new Aggregator(new Aggregation[]{aggregations[0], aggregations[1]});
        Aggregator right = new Aggregator(new Aggregation[]{aggregations[2], aggregations[3]});

        left.combine(right);

        verify(aggregations[0]).combine(aggregations[2]);
        verify(aggregations[1]).combine(aggregations[3]);
    }

    @Test
    public void test_collect() {
        Aggregator aggregator = new Aggregator(new Aggregation[]{aggregations[0], aggregations[1]});
        given(aggregations[0].collect()).willReturn(1L);
        given(aggregations[1].collect()).willReturn("v");

        assertThat(aggregator.collect()).isEqualTo(new Object[]{1L, "v"});
    }

    @Test
    public void test_serialization() {
        ValueAggregation original = new ValueAggregation(0, QueryDataType.VARCHAR);
        original.accumulate(new Object[]{"v"});

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        ValueAggregation serialized = ss.toObject(ss.toData(original));

        assertThat(serialized).isEqualTo(original);
    }
}
