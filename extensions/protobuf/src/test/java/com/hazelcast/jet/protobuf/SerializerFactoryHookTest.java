/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.protobuf;

import com.hazelcast.jet.impl.serialization.SerializerFactory;
import com.hazelcast.jet.protobuf.Messages.Person;
import com.hazelcast.nio.serialization.Serializer;
import org.junit.Test;

import static java.lang.Thread.currentThread;
import static org.assertj.core.api.Assertions.assertThat;

public class SerializerFactoryHookTest {

    @Test
    public void when_hookIsPresent_then_loadsFactory() {
        // Given
        SerializerFactory serializerFactory = new SerializerFactory(currentThread().getContextClassLoader());

        // When
        Serializer serializer = serializerFactory.createProtoSerializer(Person.class.getName(), 1);

        // Then
        assertThat(serializer).isInstanceOf(ProtoStreamSerializer.class);
    }
}
