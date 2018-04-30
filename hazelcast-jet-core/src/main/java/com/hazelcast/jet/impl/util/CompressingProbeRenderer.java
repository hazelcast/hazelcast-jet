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

package com.hazelcast.jet.impl.util;

import com.hazelcast.internal.metrics.renderers.ProbeRenderer;
import com.hazelcast.nio.Bits;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class CompressingProbeRenderer implements ProbeRenderer {

    public static final int TYPE_LONG = 0;
    public static final int TYPE_DOUBLE = 1;
    private static final short SHORT_BITS = 8 * Bits.SHORT_SIZE_IN_BYTES;

    private DataOutputStream dos;
    private ByteArrayOutputStream baos;
    private String lastName = "";

    public CompressingProbeRenderer(int estimatedBytes) {
        baos = new ByteArrayOutputStream(estimatedBytes);
        dos = new DataOutputStream(baos);
    }

    @Override
    public void renderLong(String name, long value) {
        try {
            writeName(name);
            dos.writeByte(TYPE_LONG);
            dos.writeLong(value);
        } catch (IOException e) {
            throw new RuntimeException(e); // should never be thrown
        }
    }

    @Override
    public void renderDouble(String name, double value) {
        try {
            writeName(name);
            dos.writeByte(TYPE_DOUBLE);
            dos.writeDouble(value);
        } catch (IOException e) {
            throw new RuntimeException(e); // should never be thrown
        }
    }

    private void writeName(String name) throws IOException {
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
    }

    @Override
    public void renderException(String name, Exception e) {
        // ignore
    }

    @Override
    public void renderNoValue(String name) {
        // ignore
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

    public static void decompress(byte[] data, ProbeRenderer renderer) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
        String lastName = "";
        try {
            while (dis.available() > 0) {
                int equalPrefixLen = dis.readUnsignedShort();
                lastName = lastName.substring(0, equalPrefixLen)
                        + dis.readUTF();
                int type = dis.readByte();
                if (type == TYPE_LONG) {
                    renderer.renderLong(lastName, dis.readLong());
                } else if (type == TYPE_DOUBLE) {
                    renderer.renderDouble(lastName, dis.readDouble());
                } else {
                    throw new RuntimeException("Unexpected type");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // can be thrown with unexpected EOFException
        }
    }
}
