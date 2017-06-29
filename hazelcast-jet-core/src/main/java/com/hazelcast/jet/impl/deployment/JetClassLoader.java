/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IMap;
import com.hazelcast.nio.IOUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.zip.InflaterInputStream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class JetClassLoader extends ClassLoader {

    private final IMap<String, Object> jobMetadataMap;

    public JetClassLoader(IMap jobMetadataMap) {
        super(JetClassLoader.class.getClassLoader());
        this.jobMetadataMap = jobMetadataMap;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        if (isEmpty(name)) {
            return null;
        }
        InputStream classBytesStream = resourceStream(name.replace('.', '/') + ".class");
        if (classBytesStream == null) {
            throw new ClassNotFoundException(name);
        }

        MorePublicByteArrayOutputStream os = new MorePublicByteArrayOutputStream();
        try {
            IOUtil.drainTo(classBytesStream, os);
        } catch (IOException e) {
            throw rethrow(e);
        }

        return defineClass(name, os.getInternalBuffer(), 0, os.size());
    }

    @Override
    protected URL findResource(String name) {
        if (isEmpty(name)) {
            return null;
        }
        // we distinguish between the case "resource found, but not accessible by URL" and "resource not found"
        if (jobMetadataMap.containsKey("res:" + name)) {
            throw new IllegalArgumentException("Resource not accessible by URL: " + name);
        }
        return null;
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        if (isEmpty(name)) {
            return null;
        }
        return resourceStream(name);
    }

    @SuppressWarnings("unchecked")
    private InputStream resourceStream(String name) {
        byte[] classData = (byte[]) jobMetadataMap.get("res:" + name);
        if (classData == null) {
            return null;
        }
        return new InflaterInputStream(new ByteArrayInputStream(classData));
    }

    public IMap<String, Object> getJobMetadataMap() {
        return jobMetadataMap;
    }

    private static class MorePublicByteArrayOutputStream extends ByteArrayOutputStream {
        public byte[] getInternalBuffer() {
            return this.buf;
        }
    }

    private static boolean isEmpty(String className) {
        return className == null || className.isEmpty();
    }
}
