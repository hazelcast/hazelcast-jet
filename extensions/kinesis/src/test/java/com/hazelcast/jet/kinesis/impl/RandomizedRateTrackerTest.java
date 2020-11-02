package com.hazelcast.jet.kinesis.impl;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RandomizedRateTrackerTest {

    @Test
    public void check() {
        RandomizedRateTracker tracker = new RandomizedRateTracker(1000, 5);
        long p1 = tracker.next();
        long p2 = tracker.next();
        long p3 = tracker.next();
        long p4 = tracker.next();
        long p5 = tracker.next();

        //total is divided up properly
        assertEquals(1000, p1 + p2 + p3 + p4 + p5);

        //returns same values in a round-robin fashion
        for (int i = 0; i < 5; i++) {
            assertEquals(p1, tracker.next());
            assertEquals(p2, tracker.next());
            assertEquals(p3, tracker.next());
            assertEquals(p4, tracker.next());
            assertEquals(p5, tracker.next());
        }
    }

}
