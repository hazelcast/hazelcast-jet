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

import com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.ByteOrder.nativeOrder;

public final class DataOutputFactory {

    private DataOutputFactory() {
    }

    public static DataOutput create(int size) {
        if (GlobalMemoryAccessorRegistry.MEM_AVAILABLE) {
            return new UnsafeDataOutput(nativeOrder() != LITTLE_ENDIAN, size);
        } else {
            return new ByteArrayDataOutput(size);
        }
    }
}
