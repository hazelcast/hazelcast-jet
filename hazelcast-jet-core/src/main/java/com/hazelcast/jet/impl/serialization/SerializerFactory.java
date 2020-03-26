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

package com.hazelcast.jet.impl.serialization;

import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.ServiceLoader;
import com.hazelcast.nio.serialization.StreamSerializer;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Optional;

import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.jet.impl.util.ReflectionUtils.loadClass;
import static com.hazelcast.jet.impl.util.ReflectionUtils.newInstance;

/**
 * A serializer factory facade that discovers serializer factories by reading
 * in the file(s) "META-INF/services/com.hazelcast.jet.*.SerializerFactoryHook".
 * <p>
 * This system is meant to be internal code and is subject to change at any time.
 */
public final class SerializerFactory {

    private static final String PROTOBUF_FACTORY_ID = "com.hazelcast.jet.protobuf.SerializerFactoryHook";

    private final ClassLoader classLoader;
    private final ProtobufSerializerFactory protobufSerializerFactory;

    public SerializerFactory(@Nonnull ClassLoader classLoader) {
        this.classLoader = classLoader;
        this.protobufSerializerFactory = loadProtobufSerializerFactory(classLoader)
                .orElseGet(() -> new ProtobufSerializerFactory() {
                    @Override
                    public <T> StreamSerializer<T> create(@Nonnull Class<T> clazz, int typeId) {
                        throw new IllegalStateException("no Protobuf serializer factory found");
                    }
                });
    }

    private static Optional<ProtobufSerializerFactory> loadProtobufSerializerFactory(ClassLoader classLoader) {
        try {
            Iterator<ProtobufSerializerFactory> factoryIterator =
                    ServiceLoader.iterator(ProtobufSerializerFactory.class, PROTOBUF_FACTORY_ID, classLoader);
            if (factoryIterator.hasNext()) {
                ProtobufSerializerFactory factory = factoryIterator.next();
                checkFalse(factoryIterator.hasNext(), "multiple Protobuf serializer factories found");
                return Optional.of(factory);
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Nonnull
    public StreamSerializer<?> createSerializer(@Nonnull String serializerClassName) {
        return newInstance(classLoader, serializerClassName);
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public <T> StreamSerializer<T> createProtobufSerializer(@Nonnull String className, int typeId) {
        Class<T> clazz = (Class<T>) loadClass(classLoader, className);
        return protobufSerializerFactory.create(clazz, typeId);
    }

    @FunctionalInterface
    public interface ProtobufSerializerFactory {

        <T> StreamSerializer<T> create(@Nonnull Class<T> clazz, int typeId);
    }
}
