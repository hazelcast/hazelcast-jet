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

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.serialization.impl.AbstractSerializationService;

public class ByteArrayDataInput implements DataInput {

    private byte[] buffer;
    private int position;

    ByteArrayDataInput(byte[] buffer) {
        this.buffer = buffer;
        this.position = 0;
    }

    @Override
    public int readInt() {
        checkAvailable(Integer.BYTES);
        int value = Bits.readInt(buffer, position, false);
        position += Integer.BYTES;
        return value;
    }

    @Override
    public long readLong() {
        checkAvailable(Long.BYTES);
        long value = Bits.readLong(buffer, position, false);
        position += Long.BYTES;
        return value;
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
    public BufferObjectDataInput toObjectInput(AbstractSerializationService serializationService) {
        byte[] bytes = new byte[buffer.length - position];
        System.arraycopy(buffer, position, bytes, 0, bytes.length);
        position = buffer.length;
        return serializationService.createObjectDataInput(bytes);
    }
}
