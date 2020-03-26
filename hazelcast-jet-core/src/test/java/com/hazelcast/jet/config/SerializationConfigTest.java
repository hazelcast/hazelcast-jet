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

package com.hazelcast.jet.config;

import com.hazelcast.jet.config.SerializationConfig.ProtobufSerializerPrimer;
import com.hazelcast.jet.config.SerializationConfig.StreamSerializerPrimer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.junit.Test;

import java.util.AbstractMap.SimpleEntry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SerializationConfigTest {

    @Test
    public void when_registersClassTwice_then_fails() {
        // Given
        SerializationConfig config = new SerializationConfig();
        config.registerSerializer(Object.class, ObjectSerializer.class);

        // When
        // Then
        assertThatThrownBy(() -> config.registerProtobufSerializer(Object.class, 1));
    }

    @Test
    public void when_registersSerializerTwice_then_fails() {
        // Given
        SerializationConfig config = new SerializationConfig();
        config.registerSerializer(Object.class, ObjectSerializer.class);

        // When
        // Then
        assertThatThrownBy(() -> config.registerSerializer(String.class, ObjectSerializer.class));
    }

    @Test
    public void when_registersSerializer() {
        // Given
        SerializationConfig config = new SerializationConfig();

        // When
        config.registerSerializer(Object.class, ObjectSerializer.class);

        // Then
        assertThat(config.isEmpty()).isFalse();
        assertThat(config.primers()).containsOnly(
                new SimpleEntry<>(Object.class.getName(), new StreamSerializerPrimer(ObjectSerializer.class.getName()))
        );
    }

    @Test
    public void when_registersProtobufSerializer() {
        // Given
        SerializationConfig config = new SerializationConfig();

        // When
        config.registerProtobufSerializer(Object.class, 1);

        // Then
        assertThat(config.isEmpty()).isFalse();
        assertThat(config.primers()).containsOnly(
                new SimpleEntry<>(Object.class.getName(), new ProtobufSerializerPrimer(Object.class.getName(), 1))
        );
    }

    private static class ObjectSerializer implements StreamSerializer<Object> {

        @Override
        public int getTypeId() {
            return 1;
        }

        @Override
        public void write(ObjectDataOutput out, Object object) {
        }

        @Override
        public Object read(ObjectDataInput in) {
            return null;
        }
    }
}
