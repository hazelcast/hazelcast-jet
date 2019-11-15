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

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class JobConfigTest extends JetTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void when_setName_thenReturnsName() {
        // When
        JobConfig config = new JobConfig();
        String name = "myJobName";
        config.setName(name);

        // Then
        assertEquals(name, config.getName());
    }

    @Test
    public void when_enableSplitBrainProtection_thenReturnsEnabled() {
        // When
        JobConfig config = new JobConfig();
        config.setSplitBrainProtection(true);

        // Then
        assertTrue(config.isSplitBrainProtectionEnabled());
    }

    @Test
    public void when_enableAutoScaling_thenReturnsEnabled() {
        // When
        JobConfig config = new JobConfig();
        config.setAutoScaling(true);

        // Then
        assertTrue(config.isAutoScaling());
    }

    @Test
    public void when_setProcessingGuarantee_thenReturnsProcessingGuarantee() {
        // When
        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(EXACTLY_ONCE);

        // Then
        assertEquals(EXACTLY_ONCE, config.getProcessingGuarantee());
    }

    @Test
    public void when_setSnapshotIntervalMillis_thenReturnsSnapshotIntervalMillis() {
        // When
        JobConfig config = new JobConfig();
        config.setSnapshotIntervalMillis(50);

        // Then
        assertEquals(50, config.getSnapshotIntervalMillis());
    }

    @Test
    public void when_addClass_thenReturnsResourceConfig() {
        // When
        JobConfig config = new JobConfig();
        Class clazz = this.getClass();
        config.addClass(clazz);

        // Then
        ResourceConfig resourceConfig = assertAndGet(config);
        assertFalse(resourceConfig.isArchive());
        String expectedId = clazz.getName().replace('.', '/') + ".class";
        assertEquals(expectedId, resourceConfig.getId());
        assertEquals(clazz.getClassLoader().getResource(expectedId), resourceConfig.getUrl());
    }


    @Test
    public void when_addJarWithPath_thenReturnsResourceConfig() throws MalformedURLException {
        // When
        JobConfig config = new JobConfig();
        String path = "/path/to/my.jar";
        config.addJar(path);

        // Then
        ResourceConfig resourceConfig = assertAndGet(config);
        assertTrue(resourceConfig.isArchive());
        assertEquals(new File(path).toURI().toURL(), resourceConfig.getUrl());
        assertNull(resourceConfig.getId());
    }

    @Test
    public void when_addJarWithFile_thenReturnsResourceConfig() throws MalformedURLException {
        // When
        JobConfig config = new JobConfig();
        File file = new File("/path/to/my.jar");
        config.addJar(file);

        // Then
        ResourceConfig resourceConfig = assertAndGet(config);
        assertTrue(resourceConfig.isArchive());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
        assertNull(resourceConfig.getId());
    }

    @Test
    public void when_addJarWithURL_thenReturnsResourceConfig() throws MalformedURLException {
        // When
        JobConfig config = new JobConfig();
        URL url = new URL("http://path.to/my.jar");
        config.addJar(url);

        // Then
        ResourceConfig resourceConfig = assertAndGet(config);
        assertTrue(resourceConfig.isArchive());
        assertEquals(url, resourceConfig.getUrl());
        assertNull(resourceConfig.getId());
    }


    @Test
    public void when_addResourceWithPath_thenReturnsResourceConfig() throws MalformedURLException {
        // When
        JobConfig config = new JobConfig();
        String path = "/path/to/my.txt";
        config.addResource(path);

        // Then
        ResourceConfig resourceConfig = assertAndGet(config);
        assertFalse(resourceConfig.isArchive());
        assertEquals(new File(path).toURI().toURL(), resourceConfig.getUrl());
        assertEquals("my.txt", resourceConfig.getId());
    }

    @Test
    public void when_addResourceWithPathAndId_thenReturnsResourceConfig() throws MalformedURLException {
        // When
        JobConfig config = new JobConfig();
        String path = "/path/to/my.txt";
        config.addResource(path, "customId");

        // Then
        ResourceConfig resourceConfig = assertAndGet(config);
        assertFalse(resourceConfig.isArchive());
        assertEquals(new File(path).toURI().toURL(), resourceConfig.getUrl());
        assertEquals("customId", resourceConfig.getId());
    }

    @Test
    public void when_addResourceWithFile_thenReturnsResourceConfig() throws MalformedURLException {
        // When
        JobConfig config = new JobConfig();
        File file = new File("/path/to/my.txt");
        config.addResource(file);

        // Then
        ResourceConfig resourceConfig = assertAndGet(config);
        assertFalse(resourceConfig.isArchive());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
        assertEquals("my.txt", resourceConfig.getId());
    }

    @Test
    public void when_addResourceWithFileAndId_thenReturnsResourceConfig() throws MalformedURLException {
        // When
        JobConfig config = new JobConfig();
        File file = new File("/path/to/my.txt");
        config.addResource(file, "customId");

        // Then
        ResourceConfig resourceConfig = assertAndGet(config);
        assertFalse(resourceConfig.isArchive());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
        assertEquals("customId", resourceConfig.getId());
    }

    @Test
    public void when_addResourceWithURL_thenReturnsResourceConfig() throws MalformedURLException {
        // When
        JobConfig config = new JobConfig();
        URL url = new URL("http://path.to/my.txt");
        config.addResource(url);

        // Then
        ResourceConfig resourceConfig = assertAndGet(config);
        assertFalse(resourceConfig.isArchive());
        assertEquals(url, resourceConfig.getUrl());
        assertEquals("my.txt", resourceConfig.getId());
    }

    @Test
    public void when_losslessRestartEnabled_then_openSourceMemberDoesNotStart() {
        // When
        JetConfig jetConfig = new JetConfig();
        jetConfig.getInstanceConfig().setLosslessRestartEnabled(true);

        // Then
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Hot Restart requires Hazelcast Enterprise Edition");
        createJetMember(jetConfig);
    }

    private ResourceConfig assertAndGet(JobConfig config) {
        List<ResourceConfig> resourceConfigs = config.getResourceConfigs();
        assertNotNull(resourceConfigs);
        assertEquals(1, resourceConfigs.size());
        return resourceConfigs.iterator().next();
    }
}
