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

import com.hazelcast.internal.metrics.renderers.ProbeRenderer;
import com.hazelcast.nio.Bits;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import static com.hazelcast.internal.metrics.MetricsUtil.escapeMetricKeyPart;

/**
 * Probe renderer to serialize metrics to byte[] to be sent to ManCenter.
 * Additionally, it converts legacy metric names to {@code [metric=<oldName>]}.
 */
public class CompressingProbeRenderer implements ProbeRenderer {


    private static final short SHORT_BITS = 8 * Bits.SHORT_SIZE_IN_BYTES;
    private static final int CONVERSION_PRECISION = 4;

    private DataOutputStream dos;
    private ByteArrayOutputStream baos;
    private String lastName = "";
    private int count;

    public CompressingProbeRenderer(int estimatedBytes) {
        baos = new ByteArrayOutputStream(estimatedBytes);
        dos = new DataOutputStream(baos);
    }

    @Override
    public void renderLong(String name, long value) {
        try {
            writeName(name);
            dos.writeLong(value);
        } catch (IOException e) {
            throw new RuntimeException(e); // should never be thrown
        }
    }

    @Override
    public void renderDouble(String name, double value) {
        try {
            writeName(name);
            // convert to long with specified precision
            dos.writeLong(Math.round(value * Math.pow(10, CONVERSION_PRECISION)));
        } catch (IOException e) {
            throw new RuntimeException(e); // should never be thrown
        }
    }

    private void writeName(String name) throws IOException {
        // Metric name should have the form "[tag1=value1,tag2=value2,...]". If it is not
        // enclosed in "[]", we convert it to "[metric=originalName]".
        if (!name.startsWith("[") || !name.endsWith("]")) {
            name = "[metric=" + escapeMetricKeyPart(name) + ']';
        }

        if (name.length() >= 1 << SHORT_BITS) {
            throw new RuntimeException("metric name too long: " + name);
        }
        // count number of shared characters
        int equalPrefixLength = 0;
        int shorterLen = Math.min(name.length(), lastName.length());
        while (equalPrefixLength < shorterLen
                && name.charAt(equalPrefixLength) == lastName.charAt(equalPrefixLength)) {
            equalPrefixLength++;
        }
        dos.writeShort(equalPrefixLength);
        dos.writeUTF(name.substring(equalPrefixLength));
        lastName = name;
        count++;
    }

    @Override
    public void renderException(String name, Exception e) {
        // ignore
    }

    @Override
    public void renderNoValue(String name) {
        // ignore
    }

    public int getCount() {
        return count;
    }

    public byte[] getRenderedBlob() {
        try {
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e); // should never be thrown
        } finally {
            dos = null;
            baos = null;
        }
    }

    static Iterator<Metric> decompressingIterator(byte[] bytes) {
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
                        next = new Metric(lastName, dis.readLong());
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
