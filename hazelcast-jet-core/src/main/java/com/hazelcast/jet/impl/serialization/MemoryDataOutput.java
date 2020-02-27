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

import javax.annotation.Nonnull;
import java.io.DataOutput;

public interface MemoryDataOutput extends DataOutput {

    @Override
    default void write(int b) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void write(@Nonnull byte[] b) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void write(@Nonnull byte[] b, int off, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void writeBoolean(boolean v) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void writeByte(int v) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void writeShort(int v) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void writeChar(int v) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void writeInt(int v) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void writeLong(long v) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void writeFloat(float v) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void writeDouble(double v) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void writeBytes(@Nonnull String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void writeChars(@Nonnull String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void writeUTF(@Nonnull String s) {
        throw new UnsupportedOperationException();
    }

    byte[] toByteArray();
}
