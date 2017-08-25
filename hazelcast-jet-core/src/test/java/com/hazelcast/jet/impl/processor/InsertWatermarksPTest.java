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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.Processor.Context;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.WatermarkEmissionPolicy;
import com.hazelcast.jet.WatermarkPolicies;
import com.hazelcast.jet.WatermarkPolicy;
import com.hazelcast.jet.test.TestOutbox;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static com.hazelcast.jet.WatermarkEmissionPolicy.emitAll;
import static com.hazelcast.jet.WatermarkPolicies.withFixedLag;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class InsertWatermarksPTest {

    private static final long LAG = 3;

    @Parameter
    public int outboxCapacity;

    private MockClock clock = new MockClock(100);
    private InsertWatermarksP<Item> p;
    private TestOutbox outbox;
    private List<Object> resultToCheck = new ArrayList<>();
    private Context context;
    private WatermarkPolicy wmPolicy = withFixedLag(LAG).get();
    private WatermarkEmissionPolicy wmEmissionPolicy = emitAll();

    @Parameters(name = "outboxCapacity={0}")
    public static Collection<Object> parameters() {
        return asList(1, 1024);
    }

    @Before
    public void setUp() {
        outbox = new TestOutbox(outboxCapacity);
        context = mock(Context.class);
    }

    @Test
    public void smokeTest() throws Exception {
        List<Object> input = new ArrayList<>();

        for (int eventTime = 10, time = (int) clock.now; eventTime < 22; eventTime++, time++) {
            input.add(tick(time));
            if (eventTime < 14 || eventTime >= 17 && eventTime <= 18) {
                input.add(item(eventTime));
                input.add(item(eventTime - 2));
            }
        }
        // input.forEach(System.out::println);

        doTest(input, asList(
                tick(100),
                wm(7),
                item(10),
                item(8),

                tick(101),
                wm(8),
                item(11),
                item(9),

                tick(102),
                wm(9),
                item(12),
                item(10),

                tick(103),
                wm(10),
                item(13),
                item(11),

                tick(104),
                tick(105),
                tick(106),
                tick(107),
                wm(11),
                wm(12),
                wm(13),
                wm(14),
                item(17),
                item(15),

                tick(108),
                wm(15),
                item(18),
                item(16),

                tick(109),
                tick(110),
                tick(111)
        ));
    }

    @Test
    public void when_firstEventLate_then_dropped() {
        wmPolicy = WatermarkPolicies.limitingTimestampAndWallClockLag(0, 0, clock::now).get();
        doTest(
                singletonList(item(clock.now - 1)),
                singletonList(wm(100)));
    }

    private void doTest(List<Object> input, List<Object> expectedOutput) {
        // wrap the emission policy to check how it's used and what it returns
        WatermarkEmissionPolicy wrappedEmissionPolicy = new WatermarkEmissionPolicy() {
            long lastReturnedValue = Long.MIN_VALUE;

            @Override
            public long nextWatermark(long lastEmittedWm, long currentWm) {
                assertTrue("nextWatermark() called with currentWm smaller than already returned value. " +
                                "lastReturnedValue=" + lastReturnedValue + ", currentWm=" + currentWm
                                + ", lastEmittedWm=" + lastEmittedWm,
                        lastEmittedWm < currentWm);
                if (lastReturnedValue > Long.MIN_VALUE) {
                    assertTrue("higher emitted value than emission policy returned, lastEmittedWm=" + lastEmittedWm
                                    + ", lastReturnedValue=" + lastReturnedValue,
                            lastEmittedWm <= lastReturnedValue);
                }
                assertTrue("superfluous call, lastReturnedValue=" + lastReturnedValue + ", currentWm=" + currentWm,
                        lastReturnedValue < currentWm);

                lastReturnedValue = wmEmissionPolicy.nextWatermark(lastEmittedWm, currentWm);

                assertTrue("emission policy returned value smaller than lastEmittedWm. lastEmittedWm=" + lastEmittedWm
                                + ", lastReturnedValue=" + lastReturnedValue,
                        lastReturnedValue > lastEmittedWm);
                return lastReturnedValue;
            }
        };
        p = new InsertWatermarksP<>(Item::getTimestamp, wmPolicy, wrappedEmissionPolicy);
        p.init(outbox, context);

        for (Object inputItem : input) {
            if (inputItem instanceof Tick) {
                clock.set(((Tick) inputItem).timestamp);
                resultToCheck.add(tick(clock.now));
                doAndDrain(p::tryProcess);
            } else {
                assertTrue(inputItem instanceof Item);
                doAndDrain(() -> uncheckCall(() -> p.tryProcess(0, inputItem)));
            }
        }

        assertEquals(listToString(expectedOutput), listToString(resultToCheck));
    }

    private void doAndDrain(BooleanSupplier action) {
        boolean done;
        do {
            done = action.getAsBoolean();
            drainOutbox();
        } while (!done);
    }

    private void drainOutbox() {
        resultToCheck.addAll(outbox.queueWithOrdinal(0));
        outbox.queueWithOrdinal(0).clear();
    }

    private String myToString(Object o) {
        return o instanceof Watermark
                ? "Watermark{timestamp=" + ((Watermark) o).timestamp() + "}"
                : o.toString();
    }

    private String listToString(List<?> actual) {
        return actual.stream().map(this::myToString).collect(Collectors.joining("\n"));
    }

    private static Item item(long timestamp) {
        return new Item(timestamp);
    }

    private static Watermark wm(long timestamp) {
        return new Watermark(timestamp);
    }

    private static Tick tick(long timestamp) {
        return new Tick(timestamp);
    }

    private static final class Tick {
        final long timestamp;

        private Tick(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "-- at " + timestamp;
        }
    }

    private static class Item {
        final long timestamp;

        Item(long timestamp) {
            this.timestamp = timestamp;
        }

        long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "Item{timestamp=" + timestamp + '}';
        }

        @Override
        public boolean equals(Object o) {
            return this == o || o instanceof Item && this.timestamp == ((Item) o).timestamp;
        }

        @Override
        public int hashCode() {
            return (int) (timestamp ^ (timestamp >>> 32));
        }
    }

    private static class MockClock {
        long now;

        MockClock(long now) {
            this.now = now;
        }

        long now() {
            return now;
        }

        void set(long newNow) {
            assert newNow >= now;
            now = newNow;
        }

    }
}
