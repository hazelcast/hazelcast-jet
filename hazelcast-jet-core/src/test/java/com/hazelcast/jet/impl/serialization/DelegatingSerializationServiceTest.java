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

import com.hazelcast.internal.serialization.impl.AbstractSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.SerializationConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.junit.Test;

import java.io.IOException;

import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DelegatingSerializationServiceTest {

    private static final AbstractSerializationService DELEGATE =
            (AbstractSerializationService) new DefaultSerializationServiceBuilder().build();

    @Test
    public void when_triesToRegisterTwoSerializersWithSameTypeId_then_Fails() {
        // Given
        SerializationConfig config = new SerializationConfig();
        config.registerSerializer(Boolean.class, CustomByteSerializer.class);
        config.registerSerializer(Byte.class, AnotherCustomByteSerializer.class);

        // When
        // Then
        assertThatThrownBy(() -> new DelegatingSerializationService(config, DELEGATE))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void when_triesToFindSerializerForUnregisteredType_then_Fails() {
        // Given
        DelegatingSerializationService service = new DelegatingSerializationService(new SerializationConfig(), DELEGATE);

        // When
        // Then
        assertThatThrownBy(() -> service.serializerFor(new Value()))
                .isInstanceOf(JetException.class);
        assertThatThrownBy(() -> service.serializerFor(Integer.MAX_VALUE))
                .isInstanceOf(JetException.class);
    }

    @Test
    public void when_multipleTypeSerializersRegistered_then_localHasPrecedence() {
        // Given
        SerializationConfig config = new SerializationConfig();
        config.registerSerializer(Byte.class, CustomByteSerializer.class);

        DelegatingSerializationService service = new DelegatingSerializationService(config, DELEGATE);

        // When
        // Then
        assertThat(service.serializerFor(CONSTANT_TYPE_BYTE).getImpl()).isInstanceOf(CustomByteSerializer.class);
        assertThat(service.serializerFor(Byte.valueOf((byte) 1)).getImpl()).isInstanceOf(CustomByteSerializer.class);
    }

    @Test
    public void when_triesToFindSerializerForNullObject_then_Succeeds() {
        // Given
        DelegatingSerializationService service = new DelegatingSerializationService(new SerializationConfig(), DELEGATE);

        // When
        // Then
        assertThat(service.serializerFor(null).getImpl()).isNotNull();
    }

    private static class Value {
    }

    private static class CustomByteSerializer implements StreamSerializer<Byte> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_BYTE;
        }

        @Override
        public void write(ObjectDataOutput output, Byte value) throws IOException {
            output.writeByte(value);
        }

        @Override
        public Byte read(ObjectDataInput input) throws IOException {
            return input.readByte();
        }
    }

    private static class AnotherCustomByteSerializer implements StreamSerializer<Byte> {

        @Override
        public int getTypeId() {
            return CONSTANT_TYPE_BYTE;
        }

        @Override
        public void write(ObjectDataOutput output, Byte value) throws IOException {
            output.writeByte(value);
        }

        @Override
        public Byte read(ObjectDataInput input) throws IOException {
            return input.readByte();
        }
    }
}
