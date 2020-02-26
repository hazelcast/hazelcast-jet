/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.serialization;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BYTE_BASE_OFFSET;

public class UnsafeDataInput implements DataInput {

    private final boolean reverse;

    private final byte[] buffer;
    private int position;

    public UnsafeDataInput(boolean reverse, byte[] buffer) {
        this.reverse = reverse;

        this.buffer = buffer;
        this.position = 0;
    }

    @Override
    public int readInt() {
        checkAvailable(Integer.BYTES);
        int value = MEM.getInt(buffer, ARRAY_BYTE_BASE_OFFSET + position);
        position += Integer.BYTES;
        return reverse ? Integer.reverseBytes(value) : value;
    }

    @Override
    public long readLong() {
        checkAvailable(Long.BYTES);
        long value = MEM.getLong(buffer, ARRAY_BYTE_BASE_OFFSET + position);
        position += Long.BYTES;
        return reverse ? Long.reverseBytes(value) : value;
    }

    private void checkAvailable(int length) {
        if (position + length > buffer.length) {
            throw new RuntimeException("Cannot read " + length + " bytes!");
        }
    }

    @Override
    public int position() {
        return position;
    }

    @Override
    public byte[] remaining() {
        byte[] bytes = new byte[buffer.length - position];
        System.arraycopy(buffer, position, bytes, 0, bytes.length);
        return bytes;
    }
}
