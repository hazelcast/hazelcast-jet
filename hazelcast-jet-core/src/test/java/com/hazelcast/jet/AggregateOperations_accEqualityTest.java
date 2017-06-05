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

import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.jet.AggregateOperations.allOf;
import static com.hazelcast.jet.AggregateOperations.averagingDouble;
import static com.hazelcast.jet.AggregateOperations.averagingLong;
import static com.hazelcast.jet.AggregateOperations.counting;
import static com.hazelcast.jet.AggregateOperations.linearTrend;
import static com.hazelcast.jet.AggregateOperations.reducing;
import static com.hazelcast.jet.AggregateOperations.summingDouble;
import static com.hazelcast.jet.AggregateOperations.summingLong;
import static com.hazelcast.jet.function.DistributedFunction.identity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class AggregateOperations_accEqualityTest {

    @Parameter
    public AggregateOperation<?, ?, ?> operation;

    @Parameters
    public static Collection<AggregateOperation<?, ?, ?>> data() {
        return Arrays.asList(
                counting(),
                summingLong(Long::longValue),
                summingDouble(Double::doubleValue),
                averagingLong(Long::longValue),
                averagingDouble(Double::doubleValue),
                linearTrend(x -> 1L, x -> 1L),
                allOf(counting(), summingLong(Long::longValue)),
                reducing(1, identity(), (a, b) -> a, (a, b) -> a)
        );
    }

    @Test
    public void testTwoAccumulatorsEqual() {
        assertTrue("this test is not needed if deduct is not implemented", operation.deductAccumulatorF() != null);

        Object accumulator1 = operation.createAccumulatorF();
        Object accumulator2 = operation.createAccumulatorF();

        assertEquals("two empty accumulators not equal", accumulator1, accumulator2);
    }
}
