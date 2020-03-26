package com.hazelcast.jet.protobuf;

import com.hazelcast.jet.impl.serialization.SerializerFactory;
import com.hazelcast.jet.protobuf.Messages.Person;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.junit.Test;

import static java.lang.Thread.currentThread;
import static org.assertj.core.api.Assertions.assertThat;

public class SerializerFactoryHookTest {

    @Test
    public void when_hookIsPresent_then_loadsFactory() {
        // Given
        SerializerFactory serializerFactory = new SerializerFactory(currentThread().getContextClassLoader());

        // When
        StreamSerializer<Person> serializer = serializerFactory.createProtobufSerializer(Person.class.getName(), 1);

        // Then
        assertThat(serializer).isInstanceOf(ProtobufStreamSerializer.class);
    }
}
