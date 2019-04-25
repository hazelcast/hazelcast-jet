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

package com.hazelcast.jet.config;

import com.hazelcast.jet.impl.util.Util;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.hazelcast.jet.config.AbstractJetConfigWithSystemPropertyTest.assertConfig;
import static com.hazelcast.jet.config.AbstractJetConfigWithSystemPropertyTest.assertDefaultMemberConfig;

public class XmlJetConfigLoadFromFileSystemOrClasspathTest extends AbstractJetConfigLoadFromFileSystemOrClasspathTest {

    private static final String TEST_XML_JET = "hazelcast-jet-test.xml";

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void when_fileSystemNullFile_thenThrowsException() throws Exception {
        JetConfig.loadFromFile((File) null);
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void when_fileSystemNullProperties_thenThrowsException() throws Exception {
        JetConfig.loadFromFile(new File("test"), null);
    }

    @Override
    @Test(expected = FileNotFoundException.class)
    public void when_fileSystemPathSpecifiedNonExistingFile_thenThrowsException() throws Exception {
        JetConfig.loadFromFile(new File("non-existent.xml"));
    }

    @Override
    public void when_fileSystemFileSpecified_usesSpecifiedFile() throws IOException {
        // Given
        File tempFile = File.createTempFile("jet", ".xml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_XML_JET);
            os.write(Util.readFully(resourceAsStream));
        }

        // When
        JetConfig jetConfig = JetConfig.loadFromFile(tempFile);

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Override
    @Test
    public void when_fileSystemPathSpecified_usesSpecifiedFile() throws IOException {
        // Given
        File tempFile = File.createTempFile("jet", ".xml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_XML_JET);
            os.write(Util.readFully(resourceAsStream));
        }

        // When
        JetConfig jetConfig = JetConfig.loadFromFile(tempFile);

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void when_classpathSpecifiedNonExistingFile_thenThrowsException() {
        JetConfig.loadFromClasspath(getClass().getClassLoader(), "non-existent.xml");
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void when_classPathNullResource_thenThrowsException() throws Exception {
        JetConfig.loadFromClasspath(getClass().getClassLoader(), null, System.getProperties());
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void when_classPathNullProperties_thenThrowsException() throws Exception {
        JetConfig.loadFromClasspath(getClass().getClassLoader(), "test", null);
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void when_classPathNullClassloader_thenThrowsException() throws Exception {
        JetConfig.loadFromClasspath(null, "test");
    }

    @Override
    @Test
    public void when_classpathSpecified_usesSpecifiedResource() {
        // When
        JetConfig jetConfig = JetConfig.loadFromClasspath(getClass().getClassLoader(), TEST_XML_JET);

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Override
    public void when_classpathSpecifiedWithClassloader_usesSpecifiedResource() {
        // When
        JetConfig jetConfig = JetConfig.loadFromClasspath(getClass().getClassLoader(), TEST_XML_JET);

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }
}
