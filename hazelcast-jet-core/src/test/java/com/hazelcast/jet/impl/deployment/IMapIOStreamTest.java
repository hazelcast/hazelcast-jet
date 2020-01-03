/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.ByteBuffer;

import static com.hazelcast.jet.impl.util.IOUtil.copyStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class IMapIOStreamTest extends JetTestSupport {

    @Test
    public void test_writeFile_to_IMap_then_fileReadFromIMapAsByteArray() throws Exception {
        // Given
        URL resource = this.getClass().getClassLoader().getResource("deployment/resource.txt");
        File file = new File(resource.toURI());
        long length = file.length();
        InputStream inputStream = resource.openStream();
        JetInstance jet = createJetMember();
        IMap<String, byte[]> map = jet.getMap(randomMapName());

        // When
        try (IMapOutputStream ios = new IMapOutputStream(map, "test")) {
            copyStream(inputStream, ios);
        }

        // Then
        byte[] bytes = map.get("test");
        Assert.assertNotNull(bytes);
        int chunks = ByteBuffer.wrap(bytes).getInt();
        assertEquals(chunks + 1, map.size());
        int lengthFromMap = 0;
        ByteBuffer byteBuffer = ByteBuffer.allocate((int) length);
        for (int i = 1; i <= chunks; i++) {
            byte[] chunk = map.get("test_" + i);
            byteBuffer.put(chunk);
            lengthFromMap += chunk.length;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(byteBuffer.array())));
        assertTrue(reader.readLine().startsWith("AAAP|Advanced"));
        assertEquals(length, lengthFromMap);
    }

    @Test
    public void test_writeFile_to_IMap_then_fileReadFromIMap_with_IMapInputStream() throws Exception {
        // Given
        URL resource = this.getClass().getClassLoader().getResource("deployment/resource.txt");
        File file = new File(resource.toURI());
        long length = file.length();
        InputStream inputStream = resource.openStream();
        JetInstance jet = createJetMember();
        IMap<String, byte[]> map = jet.getMap(randomMapName());

        // When
        try (IMapOutputStream ios = new IMapOutputStream(map, "test")) {
            copyStream(inputStream, ios);
        }

        // Then
        try (IMapInputStream iMapInputStream = new IMapInputStream(map, "test")) {
            byte[] bytes = new byte[(int) length];
            BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes)));
            assertTrue(IOUtil.readFullyOrNothing(iMapInputStream, bytes));
            assertTrue(reader.readLine().startsWith("AAAP|Advanced"));
        }
    }

}
