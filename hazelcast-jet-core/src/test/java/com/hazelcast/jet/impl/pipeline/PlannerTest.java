/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.pipeline;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PlannerTest {

    @Test
    public void when_allOffsetsEqual_then_offsetIgnored() {
        assertArrayEquals(
                frameDef(5, 3),
                Planner.calculateGcd(asList(
                        frameDef(10, 3),
                        frameDef(20, 3),
                        frameDef(35, 3)
                )));
    }

    @Test
    public void when_offsetsDifferent_then_offsetsIncluded() {
        assertArrayEquals(
                frameDef(250, 0),
                Planner.calculateGcd(asList(
                        frameDef(1000, 250),
                        frameDef(1500, 750),
                        frameDef(1000, 250)
                )));
    }

    @Test
    public void when_zeroFrameDefs_then_zeroFrameDef() {
        assertArrayEquals(frameDef(0, 0), Planner.calculateGcd(Collections.emptyList()));
    }

    @Test
    public void when_allZeroFrames_then_zeroFrameDef() {
        assertArrayEquals(frameDef(0, 0), Planner.calculateGcd(asList(frameDef(0, 0))));
    }

    /**
     * Generate random frame requirements and assert that their watermarks are
     * on the verge of calculated frame length.
     */
    @Test
    public void calculateGcd_randomTest() {
        Random rnd = new Random(123); // use fixed seed to have repeatable test
        for (int iteration = 0; iteration < 1000; iteration++) {
            List<long[]> frames = new ArrayList<>();
            for (int i = 0; i < rnd.nextInt(16); i++) {
                frames.add(frameDef(rnd.nextInt(10), rnd.nextInt(10)));
            }

            long[] result = Planner.calculateGcd(frames);
            System.out.println(String.format("Result {%d, %d} for %d frameDefs: %s", result[0], result[1], frames.size(),
                    frames.stream().map(Arrays::toString).collect(joining(", ", "[", "]"))));

            if (result[0] == 0) {
                assertTrue("all frames should have been {0, 0} for result {0, x}", frames.stream().allMatch(f -> f[0] == 0));
            } else {
                for (long[] frame : frames) {
                    assertEquals(
                            String.format("Frame {%d, %d} is not divisible by result {%d, %d}",
                                    frame[0], frame[1], result[0], result[1]),
                            0, (frame[0] + frame[1] - result[1]) % result[0]);
                }
            }
        }
    }

    private long[] frameDef(long frameLength, long offset) {
        return new long[]{frameLength, offset};
    }

}
