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