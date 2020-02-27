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

import org.junit.Test;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.ByteOrder.nativeOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assume.assumeTrue;

public class UnsafeDataOutputTest {

    @Test
    public void whenBufferIsTooSmall_thenExtendsIt() {
        // Given
        DataOutput output = new UnsafeDataOutput(0);

        // When
        output.writeInt(1);
        output.writeLong(2);

        // Then
        assertArrayEquals(
                output.toByteArray(),
                new byte[]{
                        1, 0, 0, 0,
                        2, 0, 0, 0, 0, 0, 0, 0
                }
        );
    }

    @Test
    public void whenBufferIsNotExhausted_thenReturnsOnlyWrittenBytes() {
        // Given
        DataOutput output = new UnsafeDataOutput(32);

        // When
        output.writeInt(1);
        output.writeLong(2);

        // Then
        assertArrayEquals(
                output.toByteArray(),
                new byte[]{
                        1, 0, 0, 0,
                        2, 0, 0, 0, 0, 0, 0, 0
                }
        );
    }

    @Test
    public void whenReverseIsSet_thenBytesAreWrittenInReverseOrder() {
        assumeTrue(nativeOrder() == LITTLE_ENDIAN);

        // Given
        DataOutput output = new UnsafeDataOutput(true, 0);

        // When
        output.writeInt(1);
        output.writeLong(2);

        // Then
        assertArrayEquals(
                output.toByteArray(),
                new byte[]{
                        0, 0, 0, 1,
                        0, 0, 0, 0, 0, 0, 0, 2
                }
        );
    }
}
