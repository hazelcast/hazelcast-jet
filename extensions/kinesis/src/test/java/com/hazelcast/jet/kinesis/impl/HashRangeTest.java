package com.hazelcast.jet.kinesis.impl;

import org.junit.Test;

import static org.junit.Assert.*;

public class HashRangeTest {

    @Test
    public void partition() {
        HashRange range = new HashRange(1000, 2000);
        assertEquals(new HashRange(1000, 1500), range.partition(0, 2));
        assertEquals(new HashRange(1500, 2000), range.partition(1, 2));
    }

    @Test
    public void contains() {
        HashRange range = new HashRange(1000, 2000);
        assertFalse(range.contains("999"));
        assertTrue(range.contains("1000"));
        assertTrue(range.contains("1999"));
        assertFalse(range.contains("2000"));
        assertFalse(range.contains("2001"));
    }
}
