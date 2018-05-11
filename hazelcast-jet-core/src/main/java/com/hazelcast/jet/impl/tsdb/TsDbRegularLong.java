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

import java.util.Arrays;

import static java.lang.System.arraycopy;

/**
 * Time-series database with regular intervals and {@code long} values.
 */
public class TsDbRegularLong {
    private final long interval;

    private long startSequence;
    private long endSequence;
    private long[] values;

    public TsDbRegularLong(long interval, int numberOfPoints) {
        this.interval = interval;
        values = new long[numberOfPoints];
    }

    /**
     * Store measurement at specified time. If the {@code timestamp} is newer
     * than the newest value, it will cause dropping of oldest values. If the
     * {@code timestamp} is not at the verge of an interval, it will be
     * interpolated.
     * <p>
     * The call will never fail: if you add a value for a time that is not
     * retained, it will be ignored.
     */
    public void storePoint(long timestamp, long value) {
        double thisSequenceD = (double) timestamp / interval;
        long thisSequenceI = timestamp / interval;
        long intervalRemainder = timestamp % interval;
        if (intervalRemainder >= interval / 2) {
            thisSequenceI++;
        }

        // initial point
        if (startSequence == endSequence) {
            values[toIndex(thisSequenceI)] = value;
            startSequence = thisSequenceI;
            endSequence = thisSequenceI + 1;
            return;
        }

        // interpolate to the future
        long newestValueSeq = endSequence - 1;
        long newestValue = values[toIndex(newestValueSeq)];
        for (int index = toIndex(endSequence); endSequence <= thisSequenceI; endSequence++, index = toIndexSimple(index + 1)) {
            double factor = (endSequence - newestValueSeq) / (thisSequenceD - newestValueSeq);
            double valueDiff = value - newestValue;
            values[index] = Math.round(factor * valueDiff + newestValue);
        }

        // interpolate to the past - only if full capacity is not used
        if (endSequence - startSequence < values.length) {
            long oldestValueSeq = startSequence;
            long oldestValue = values[toIndex(oldestValueSeq)];
            long newStartSequence = Math.max(endSequence - values.length, thisSequenceI);
            for (int index = toIndex(startSequence - 1); startSequence > newStartSequence; startSequence--, index = toIndexSimple(index - 1)) {
                // we interpolate the value at (startSequence - 1)
                double factor = (oldestValueSeq - (startSequence - 1)) / (oldestValueSeq - thisSequenceD);
                double valueDiff = oldestValue - value;
                values[index] = Math.round(oldestValue - factor * valueDiff);
            }
        }

        // interpolate/update in the middle
        if (intervalRemainder == 0) {
            values[toIndex(thisSequenceI)] = value;
        } else if (intervalRemainder < interval / 2 && thisSequenceI > startSequence) {
            long previousValue = values[toIndex(thisSequenceI - 1)];
            double factor = (thisSequenceD - thisSequenceI) / (thisSequenceD - (thisSequenceI - 1));
            values[toIndex(thisSequenceI)] = Math.round(value - (value - previousValue) * factor);
        } else {
            long nextValue = values[toIndex(thisSequenceI  + 1)];
            double factor = (thisSequenceI - thisSequenceD) / (thisSequenceI + 1 - thisSequenceD);
            values[toIndex(thisSequenceI)] = Math.round(value + (nextValue - value) * factor);
        }
    }

    /**
     * Returns the closest value at desired timestamp.
     *
     * @param noValueMarker Value to return if the value is not available.
     */
    public long query(long timestamp, long noValueMarker) {
        long thisSequenceI = timestamp / interval;

        if (thisSequenceI < startSequence || thisSequenceI >= endSequence) {
            return noValueMarker;
        }
        return values[toIndex(thisSequenceI)];
    }

    /**
     * Returns a slice for time range.
     *
     * @param startTs Start timestamp (inclusive)
     * @param endTs End timestamp (exclusive)
     * @param noValueMarker Value to return if the value is not available.
     */
    public long[] queryRange(final long startTs, final long endTs, long noValueMarker) {
        if (endTs < startTs) {
            throw new IllegalArgumentException("Negative range");
        }
        long startSeq = startTs / interval;
        long endSeq = endTs / interval;
        assert  endSeq - startSeq <= Integer.MAX_VALUE : "too many values";
        long[] res = new long[(int) (endSeq - startSeq)];
        int offset = 0;
        // special case if no data point was added
        if (startSequence == endSequence) {
            Arrays.fill(res, noValueMarker);
            return res;
        }

        // fill marker for non-retained past
        if (startSeq < startSequence) {
            offset += Math.min((int) (startSequence - startSeq), res.length);
            if (noValueMarker != 0) {
                Arrays.fill(res, 0, offset, noValueMarker);
            }
            startSeq = startSequence;
        }
        if (startSeq >= endSeq) {
            return res;
        }
        if (startSeq < endSequence) { // if there is something to copy from the actual values
            int startIndex = toIndex(startSeq);
            int endIndex = toIndex(Math.min(endSeq, endSequence));
            if (startIndex < endIndex) {
                arraycopy(values, startIndex, res, offset, endIndex - startIndex);
                offset += endIndex - startIndex;
            } else {
                arraycopy(values, startIndex, res, offset, values.length - startIndex);
                offset += values.length - startIndex;
                arraycopy(values, 0, res, offset, endIndex);
                offset += endIndex;
            }
        }
        // fill marker for non-added future
        if (endSeq > endSequence) {
            assert res.length - offset <= endSeq - endSequence : "res.length=" + res.length + ", offset=" + offset
                    + ", endSeq=" + endSeq + ", this.endSequence=" + endSequence;
            if (noValueMarker != 0) {
                Arrays.fill(res, offset, res.length, noValueMarker);
            }
        }

        return res;
    }

    public long interval() {
        return interval;
    }

    private int toIndex(long sequence) {
        return (int) Math.floorMod(sequence, values.length);
    }

    /**
     * Accepts sequence in range 0 .. 2*values.length and
     * returns equivalent of {@code toIndex(sequence)}
     */
    private int toIndexSimple(int index) {
        assert index >= 0 && index < 2 * values.length : "index=" + index + ", values.length=" + values.length;
        if (index >= values.length) {
            return index - values.length;
        }
        return index;
    }
}
