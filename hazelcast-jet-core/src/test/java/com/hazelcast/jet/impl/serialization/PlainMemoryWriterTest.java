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

import static org.assertj.core.api.Assertions.assertThat;

public class PlainMemoryWriterTest {

    @Test
    public void when_Writes_then_BytesAreCorrectlyStored() {
        // Given
        MemoryWriter writer = new PlainMemoryWriter();
        byte[] bytes = new byte[Integer.BYTES + Long.BYTES];

        // When
        writer.writeInt(bytes, 0, 1);
        writer.writeLong(bytes, Integer.BYTES, 2);

        // Then
        assertThat(bytes).isEqualTo(new byte[]{
                1, 0, 0, 0,
                2, 0, 0, 0, 0, 0, 0, 0
        });
    }
}
