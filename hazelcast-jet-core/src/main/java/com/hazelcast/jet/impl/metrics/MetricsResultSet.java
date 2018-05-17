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

package com.hazelcast.jet.impl.metrics;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.metrics.CompressingProbeRenderer.TYPE_DOUBLE;
import static com.hazelcast.jet.impl.metrics.CompressingProbeRenderer.TYPE_LONG;

public class MetricsResultSet  {

    private final long nextSequence;
    private final List<MetricsCollection> collections;

    public MetricsResultSet(ConcurrentArrayRingbuffer.RingbufferSlice<Map.Entry<Long, byte[]>> slice) {
        this.nextSequence = slice.nextSequence();
        this.collections =  slice.elements()
                .stream()
                .map(e -> new MetricsCollection(e.getKey(), e.getValue())).collect(Collectors.toList());
    }

    /**
     * The next sequence to read from.
     */
    public long nextSequence() {
        return nextSequence;
    }

    public List<MetricsCollection> collections() {
        return collections;
    }

    /**
     * Deserializing iterator for reading metrics
     */
    public static class MetricsCollection implements Iterable<Metric> {

        private long timestamp;
        private byte[] bytes;

        public MetricsCollection(long timestamp, byte[] bytes) {
            this.timestamp = timestamp;
            this.bytes = bytes;
        }

        public long timestamp() {
            return timestamp;
        }

        @Nonnull
        @Override
        public Iterator<Metric> iterator() {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
            return new Iterator<Metric>() {
                String lastName = "";
                Metric next;
                {
                    moveNext();
                }

                @Override
                public boolean hasNext() {
                    return next != null;
                }

                @Override
                public Metric next() {
                    try {
                        return next;
                    } finally {
                        moveNext();
                    }
                }

                private void moveNext() {
                    try {
                        if (dis.available() > 0) {
                            int equalPrefixLen = dis.readUnsignedShort();
                            lastName = lastName.substring(0, equalPrefixLen) + dis.readUTF();
                            int type = dis.readByte();
                            if (type == TYPE_LONG) {
                                next = new Metric(lastName, dis.readLong());
                            } else if (type == TYPE_DOUBLE) {
                                next = new Metric(lastName, dis.readDouble());
                            } else {
                                throw new RuntimeException("Unexpected type");
                            }
                        } else {
                            next = null;
                            dis.close();
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e); // unexpected EOFException can occur here
                    }
                }
            };
        }
    }

    public static class Metric {

        private String key;
        private Number value;

        Metric(String key, Number value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return key;
        }

        public Number value() {
            return value;
        }

        @Override
        public String toString() {
            return key + "=" + value;
        }
    }
}

