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

package com.hazelcast.jet.impl;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.nio.IOUtil;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.zip.DeflaterOutputStream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class ResourceUploader {

    private static final int MAP_PUT_BATCH_SIZE_BYTES = 1 << 18; // 256kB

    private final IMap<String, Object> targetMap;
    private final Map<String, byte[]> tmpMap = new HashMap<>();
    private int tmpMapBytes;

    public ResourceUploader(JetInstance instance, long executionId) {
        targetMap = instance.getMap(JetService.METADATA_MAP_PREFIX + executionId);
    }

    public void uploadMetadata(JobConfig jobConfig) {
        if (!targetMap.isEmpty()) {
            throw new IllegalStateException("Map not empty: " + targetMap.getName());
        }

        try {
            for (ResourceConfig rc : jobConfig.getResourceConfigs()) {
                if (rc.isArchive()) {
                    loadJar(rc.getUrl());
                } else {
                    readStreamAndPutCompressedToMap(rc.getUrl().openStream(), rc.getId());
                }
            }

            // finish the last batch
            targetMap.putAll(tmpMap);
        } catch (Throwable e) {
            try {
                targetMap.destroy();
            } catch (Throwable ignored) {
                // ignore failure in cleanup in exception handler
            }

            throw rethrow(e);
        }
    }

    /**
     * Unzips the Jar archive and processes individual entries using
     * {@link #readStreamAndPutCompressedToMap(InputStream, String)}.
     */
    private void loadJar(URL url) throws IOException {
        try (JarInputStream jis = new JarInputStream(new BufferedInputStream(url.openStream()))) {
            JarEntry jarEntry;
            while ((jarEntry = jis.getNextJarEntry()) != null) {
                if (jarEntry.isDirectory()) {
                    continue;
                }
                readStreamAndPutCompressedToMap(jis, jarEntry.getName());
            }
        }
    }

    private void readStreamAndPutCompressedToMap(InputStream in, String resourceId) throws IOException {
        String key = "res:" + resourceId;
        // ignore duplicates: the first resource in first jar takes precedence
        if (targetMap.containsKey(key)) {
            return;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DeflaterOutputStream compressor = new DeflaterOutputStream(baos);
        IOUtil.drainTo(in, compressor);
        compressor.close();
        byte[] data = baos.toByteArray();

        tmpMap.put(key, data);
        tmpMapBytes += data.length;
        if (tmpMapBytes >= MAP_PUT_BATCH_SIZE_BYTES) {
            targetMap.putAll(tmpMap);
            tmpMap.clear();
        }
    }
}
