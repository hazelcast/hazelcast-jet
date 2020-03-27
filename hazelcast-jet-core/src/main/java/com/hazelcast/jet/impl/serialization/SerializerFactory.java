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

import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.ServiceLoader;
import com.hazelcast.nio.serialization.Serializer;

import javax.annotation.Nonnull;
import java.util.Iterator;

import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.jet.impl.util.ReflectionUtils.loadClass;
import static com.hazelcast.jet.impl.util.ReflectionUtils.newInstance;

/**
 * A serializer factory facade that discovers serializer factories by reading:
 * <ul>
 *     <li>"META-INF/services/com.hazelcast.jet.protobuf.SerializerFactoryHook"
 *     for Google Protocol Buffers v3 serializer factory</li>
 * </ul>
 * <p>
 * This system is meant to be internal code and is subject to change at any time.
 */
public final class SerializerFactory {

    private static final String PROTO_FACTORY_ID = "com.hazelcast.jet.protobuf.SerializerFactoryHook";

    private final ClassLoader classLoader;
    private final ProtoSerializerFactory protoSerializerFactory;

    public SerializerFactory(@Nonnull ClassLoader classLoader) {
        this.classLoader = classLoader;
        this.protoSerializerFactory = loadProtoSerializerFactory(classLoader);
    }

    private static ProtoSerializerFactory loadProtoSerializerFactory(ClassLoader classLoader) {
        try {
            Iterator<ProtoSerializerFactory> factoryIterator =
                    ServiceLoader.iterator(ProtoSerializerFactory.class, PROTO_FACTORY_ID, classLoader);
            if (factoryIterator.hasNext()) {
                ProtoSerializerFactory factory = factoryIterator.next();
                checkFalse(factoryIterator.hasNext(), "multiple Proto serializer factories found");
                return factory;
            } else {
                return null;
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Nonnull
    public Serializer createSerializer(@Nonnull String serializerClassName) {
        return newInstance(classLoader, serializerClassName);
    }

    @Nonnull
    public Serializer createProtoSerializer(@Nonnull String className, int typeId) {
        checkState(protoSerializerFactory != null, "no Proto serializer factory found");
        return protoSerializerFactory.create(loadClass(classLoader, className), typeId);
    }

    @FunctionalInterface
    public interface ProtoSerializerFactory {

        <T> Serializer create(@Nonnull Class<T> clazz, int typeId);
    }
}
