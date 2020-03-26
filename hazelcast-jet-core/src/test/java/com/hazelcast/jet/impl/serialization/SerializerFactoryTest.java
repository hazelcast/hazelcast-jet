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

import static java.lang.Thread.currentThread;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SerializerFactoryTest {

    @Test
    public void when_hooksAreMissing_then_installsExceptionFallback() {
        // Given
        SerializerFactory serializerFactory = new SerializerFactory(currentThread().getContextClassLoader());

        // When
        // Then
        assertThatThrownBy(() -> serializerFactory.createProtobufSerializer(Object.class.getName(), 1))
                .isInstanceOf(IllegalStateException.class);
    }
}
