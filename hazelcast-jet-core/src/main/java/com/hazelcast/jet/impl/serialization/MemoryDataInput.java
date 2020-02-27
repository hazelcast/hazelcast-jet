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

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.serialization.InternalSerializationService;

import javax.annotation.Nonnull;
import java.io.DataInput;

public interface MemoryDataInput extends DataInput {

    @Override
    default void readFully(@Nonnull byte[] b) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void readFully(@Nonnull byte[] b, int off, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int skipBytes(int n) {
        throw new UnsupportedOperationException();
    }

    @Override
    default boolean readBoolean() {
        throw new UnsupportedOperationException();
    }

    @Override
    default byte readByte() {
        throw new UnsupportedOperationException();
    }

    @Override
    default int readUnsignedByte() {
        throw new UnsupportedOperationException();
    }

    @Override
    default short readShort() {
        throw new UnsupportedOperationException();
    }

    @Override
    default int readUnsignedShort() {
        throw new UnsupportedOperationException();
    }

    @Override
    default char readChar() {
        throw new UnsupportedOperationException();
    }

    @Override
    default int readInt() {
        throw new UnsupportedOperationException();
    }

    @Override
    default long readLong() {
        throw new UnsupportedOperationException();
    }

    @Override
    default float readFloat() {
        throw new UnsupportedOperationException();
    }

    @Override
    default double readDouble() {
        throw new UnsupportedOperationException();
    }

    @Override
    default String readLine() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    default String readUTF() {
        throw new UnsupportedOperationException();
    }

    BufferObjectDataInput toObjectInput(InternalSerializationService serializationService);
}
