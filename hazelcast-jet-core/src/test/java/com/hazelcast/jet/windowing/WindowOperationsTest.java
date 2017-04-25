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

package com.hazelcast.jet.windowing;

import com.hazelcast.jet.Distributed.BinaryOperator;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.windowing.WindowOperations.counting;
import static com.hazelcast.jet.windowing.WindowOperations.reducing;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WindowOperationsTest extends JetTestSupport {

    @Test
    public void when_reducing() {
        validateReduction((WindowOperation<String, Integer[], Integer>)
                        reducing(0, (String s) -> Integer.valueOf(s), Integer::sum, (x, y) -> x - y),
                "1", 1, 2, 1);
    }

    @Test
    public void when_counting() {
        validateReduction(
                (WindowOperation<Object, Object[], Long>) counting(),
                new Object(), 1L, 2L, 1L
        );
    }

    private static <T, A, R> void validateReduction(
            WindowOperation<T, A[], R> op, T item, A accedVal, A combinedVal, R finishedVal
    ) {
        BinaryOperator<A[]> deductAccF = op.deductAccumulatorF();
        assertNotNull(deductAccF);

        // When - Then
        A[] acc1 = op.createAccumulatorF().get();
        op.accumulateItemF().accept(acc1, item);


        A[] acc2 = op.createAccumulatorF().get();
        op.accumulateItemF().accept(acc2, item);

        // Checks must be made here because combine/deduct
        // are allowed to be destructive ops
        assertEquals(accedVal, acc1[0]);
        assertEquals(accedVal, acc2[0]);

        A[] combined = op.combineAccumulatorsF().apply(acc1, acc2);
        assertEquals(combinedVal, combined[0]);

        A[] deducted = deductAccF.apply(combined, acc2);
        assertEquals(accedVal, combined[0]);

        R finished = op.finishAccumulationF().apply(deducted);
        assertEquals(finishedVal, finished);
    }
}
