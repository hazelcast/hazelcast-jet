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

public class YamlJetConfigLoadFromFileSystemOrClasspathTest extends AbstractJetConfigLoadFromFileSystemOrClasspathTest {

    public static final String TEST_YAML_JET = "hazelcast-jet-test.yaml";

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void when_fileSystemNullFile_thenThrowsException() throws Exception {
        new FileSystemYamlJetConfig((File) null);
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void when_fileSystemNullProperties_thenThrowsException() throws Exception {
        new FileSystemYamlJetConfig("test", null);
    }

    @Override
    @Test(expected = FileNotFoundException.class)
    public void when_fileSystemPathSpecifiedNonExistingFile_thenThrowsException() throws Exception {
        new FileSystemYamlJetConfig("non-existent.yaml");
    }

    @Override
    public void when_fileSystemFileSpecified_usesSpecifiedFile() throws IOException {
        // Given
        File tempFile = File.createTempFile("jet", ".yaml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_YAML_JET);
            os.write(Util.readFully(resourceAsStream));
        }

        // When
        JetConfig jetConfig = new FileSystemYamlJetConfig(tempFile);

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Override
    @Test
    public void when_fileSystemPathSpecified_usesSpecifiedFile() throws IOException {
        // Given
        File tempFile = File.createTempFile("jet", ".yaml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_YAML_JET);
            os.write(Util.readFully(resourceAsStream));
        }

        // When
        JetConfig jetConfig = new FileSystemYamlJetConfig(tempFile.getAbsolutePath());

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void when_classpathSpecifiedNonExistingFile_thenThrowsException() {
        new ClasspathYamlJetConfig("non-existent.yaml");
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void when_classPathNullResource_thenThrowsException() throws Exception {
        new ClasspathYamlJetConfig(null, System.getProperties());
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void when_classPathNullProperties_thenThrowsException() throws Exception {
        new ClasspathYamlJetConfig("test", null);
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void when_classPathNullClassloader_thenThrowsException() throws Exception {
        new ClasspathYamlJetConfig(null, "test");
    }

    @Override
    @Test
    public void when_classpathSpecified_usesSpecifiedResource() {
        // When
        JetConfig jetConfig = new ClasspathYamlJetConfig(TEST_YAML_JET);

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Override
    public void when_classpathSpecifiedWithClassloader_usesSpecifiedResource() {
        // When
        JetConfig jetConfig = new ClasspathYamlJetConfig(this.getClass().getClassLoader(), TEST_YAML_JET);

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

}
