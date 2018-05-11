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

package com.hazelcast.jet.impl.tsdb;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TsDbRegularLongTest {

    private static final long NO_VALUE_MARKER = 10471028572380L;

    private TsDbRegularLong db = new TsDbRegularLong(10, 5);

    @Test
    public void test_interpolateToFuture() {
        db.storePoint(1000, 0);
        db.storePoint(1030, 30);
        assertValues(1000, 0, 10, 20, 30);
    }

    @Test
    public void test_interpolateToPast() {
        db.storePoint(1000, 30);
        db.storePoint(970, 0);
        assertValues(970, 0, 10, 20, 30);
    }

    @Test
    public void test_interpolateMiddleLeft() {
        db.storePoint(1000, 0);
        db.storePoint(1030, 30);
        db.storePoint(1024, 23);
        assertValues(1000, 0, 10, 19, 30);
    }

    @Test
    public void test_interpolateMiddleRight() {
        db.storePoint(1000, 0);
        db.storePoint(1030, 30);
        db.storePoint(1015, 13);
        assertValues(1000, 0, 10, 19, 30);
    }

    @Test
    public void test_justAfterLast() {
        db.storePoint(1000, 0);
        db.storePoint(1030, 30);
        db.storePoint(1031, 33);
        assertValues(1000, 0, 10, 20, 32);
    }

    @Test
    public void test_queryAllBefore() {
        db.storePoint(1000, 0);
        Assert.assertArrayEquals(new long[]{NO_VALUE_MARKER}, db.queryRange(0, 10, NO_VALUE_MARKER));
    }

    @Test
    public void test_queryAllAfter() {
        db.storePoint(1000, 0);
        Assert.assertArrayEquals(new long[]{NO_VALUE_MARKER}, db.queryRange(2000, 2010, NO_VALUE_MARKER));
    }

    @Test
    public void test_queryEmpty() {
        long[] expected = new long[(int) (100 / db.interval())];
        Arrays.fill(expected, NO_VALUE_MARKER);
        Assert.assertArrayEquals(expected, db.queryRange(0, 100, NO_VALUE_MARKER));
    }

    @Test
    public void test_negativeTimestamp() {
        db.storePoint(-1000, 0);
        db.storePoint(-970, 30);
        assertValues(-1000, 0, 10, 20, 30);
    }

    @Test
    public void test_largeTimestamp() {
        // if double arithmetic is used for interpolation, timestamps above 53 bits cannot
        // be represented precisely using double type.
        for (int bits = 10; bits < 63; bits++) {
            try {
                // interval 16 = 4 bits off
                db = new TsDbRegularLong(16, 5);
                long timestamp = 1L << bits + 0b11000;
                db.storePoint(timestamp, 0);
                db.storePoint(timestamp + 16 * 3, 30);
                assertValues(timestamp, 0, 10, 20, 30);
            } catch (Throwable e) {
                throw new AssertionError("Failed at " + bits + " bits", e);
            }
        }
    }

    private void assertValues(long oldestTs, long ... values) {
        Assert.assertEquals("oldestTs not at a start of an interval", 0, oldestTs % db.interval());

        // assert the bulk query result with exactly the stored range
        Assert.assertArrayEquals(values, db.queryRange(oldestTs, oldestTs + values.length * db.interval(), NO_VALUE_MARKER));

        // assert the bulk query result with extra values at both ends
        long[] expectedQueryResult = new long[values.length + 2];
        expectedQueryResult[0] = expectedQueryResult[expectedQueryResult.length - 1] = NO_VALUE_MARKER;
        System.arraycopy(values, 0, expectedQueryResult, 1, values.length);
        Assert.assertArrayEquals(expectedQueryResult,
                db.queryRange(oldestTs - db.interval(), oldestTs + (values.length + 1) * db.interval(), NO_VALUE_MARKER));

        // assert individual query result
        Assert.assertEquals("value before oldestTs not out-of-range", NO_VALUE_MARKER, queryOneValue(oldestTs - db.interval()));
        Assert.assertEquals("value after newestTs not out-of-range", NO_VALUE_MARKER,
                queryOneValue(oldestTs + values.length * db.interval()));

        for (long ts = oldestTs, i = 0; i < values.length; i++, ts += db.interval()) {
            Assert.assertEquals("unexpected value at ts=" + ts, values[(int) i], queryOneValue(ts));
        }
    }

    private long queryOneValue(long ts) {
        // get the series value in two ways - by querying directly and by querying a range with length of 1
        long v1 = db.query(ts, NO_VALUE_MARKER);
        long[] v2 = db.queryRange(ts, ts + db.interval(), NO_VALUE_MARKER);
        Assert.assertEquals("Unexpected returned array length", 1, v2.length);
        Assert.assertEquals("Same timestamp queried in two ways has different value", v1, v2[0]);
        return v1;
    }
}
