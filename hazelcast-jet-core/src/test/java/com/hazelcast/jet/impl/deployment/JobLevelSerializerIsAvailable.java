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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.jet.core.AbstractProcessor;

import javax.annotation.Nonnull;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

public class JobLevelSerializerIsAvailable extends AbstractProcessor {

    static final String VALUE_CLASS_NAME = "com.sample.serializer.Value";
    static final String SERIALIZER_CLASS_NAME = "com.sample.serializer.ValueSerializer";

    @Override
    protected void init(@Nonnull Context context) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Class<?> clazz = cl.loadClass(VALUE_CLASS_NAME);
            Object value = clazz.getDeclaredConstructor().newInstance();
            // We serialize an object so the job level serializer is invoked.
            byte[] bytes = context.serializationService().toBytes(value);
            assertArrayEquals(bytes, new byte[]{0, 0, 0, 0, 0, 0, 0, 42});
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
