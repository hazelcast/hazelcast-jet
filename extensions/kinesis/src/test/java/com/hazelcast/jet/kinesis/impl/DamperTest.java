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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DamperTest {

    private static final int K = 10;
    private static final Integer[] OUTPUTS = {500, 400, 300, 200, 100};

    private Damper<Integer> absorber = new Damper<>(K, OUTPUTS);

    @Test
    public void oneErrorAndBack() {
        assertOutput(500);

        absorber.error();
        assertOutput(400);

        for (int i = 0; i < K - 1; i++) {
            absorber.ok();
            assertOutput(400);
        }

        absorber.ok();
        assertOutput(500);
    }

    @Test
    public void twoErrorsAndBack() {
        assertOutput(500);

        absorber.error();
        assertOutput(400);

        absorber.error();
        assertOutput(300);

        for (int i = 0; i < K - 1; i++) {
            absorber.ok();
            assertOutput(300);
        }

        for (int i = 0; i < K; i++) {
            absorber.ok();
            assertOutput(400);
        }

        absorber.ok();
        assertOutput(500);
    }

    @Test
    public void lotsOfErrors() {
        assertOutput(500);

        absorber.error();
        assertOutput(400);

        absorber.error();
        assertOutput(300);

        absorber.error();
        assertOutput(200);

        for (int i = 0; i < OUTPUTS.length; i++) {
            absorber.error();
            assertOutput(100);
        }
    }

    @Test
    public void backAndForth() {
        assertOutput(500);

        absorber.error();
        assertOutput(400);

        absorber.ok();
        absorber.ok();
        absorber.ok();
        assertOutput(400);

        absorber.error();
        assertOutput(300);

        absorber.error();
        assertOutput(200);

        absorber.ok();
        absorber.ok();
        absorber.ok();
        assertOutput(200);

        absorber.error();
        assertOutput(100);

        absorber.error();
        assertOutput(100);

        for (int i = 0; i < K - 1; i++) {
            absorber.ok();
            assertOutput(100);
        }

        absorber.ok();
        assertOutput(200);
    }

    private void assertOutput(int output) {
        assertEquals(output, absorber.output().intValue());
    }
}
