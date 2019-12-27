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

import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class ResourceConfigTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testAddClass_with_Class() {
        JobConfig config = new JobConfig();
        config.addClass(this.getClass());
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(this.getClass().getName().replace('.', '/') + ".class", resourceConfig.getId());
        assertEquals(ResourceType.CLASS, resourceConfig.getResourceType());
    }

    @Test
    public void testAddJar_with_Url() throws Exception {
        JobConfig config = new JobConfig();
        String urlString = "file://path/to/jarfile";
        config.addJar(new URL(urlString));
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("jarfile", resourceConfig.getId());
        assertEquals(ResourceType.JAR, resourceConfig.getResourceType());
        assertEquals(urlString, resourceConfig.getUrl().toString());
    }

    @Test
    public void testAddJar_with_MalformedUrl_throws_Exception() throws Exception {
        expectedException.expect(MalformedURLException.class);
        JobConfig config = new JobConfig();
        String urlString = "filezzz://path/to/jarFile";
        config.addJar(new URL(urlString));
    }

    @Test
    public void testAddJar_with_Path() throws Exception {
        JobConfig config = new JobConfig();
        String path = "/path/to/jarfile";
        config.addJar(path);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("jarfile", resourceConfig.getId());
        assertEquals(ResourceType.JAR, resourceConfig.getResourceType());
        assertEquals(new File(path).toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAddJar_with_File() throws Exception {
        JobConfig config = new JobConfig();
        File file = new File("/path/to/jarfile");
        config.addJar(file);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("jarfile", resourceConfig.getId());
        assertEquals(ResourceType.JAR, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAddZipOfJar_with_Url() throws Exception {
        JobConfig config = new JobConfig();
        String urlString = "file://path/to/zipFile";
        config.addJarsInZip(new URL(urlString));
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("zipFile", resourceConfig.getId());
        assertEquals(ResourceType.JARS_IN_ZIP, resourceConfig.getResourceType());
        assertEquals(urlString, resourceConfig.getUrl().toString());
    }

    @Test
    public void testAddZipOfJar_with_MalformedUrl_throws_Exception() throws Exception {
        expectedException.expect(MalformedURLException.class);
        JobConfig config = new JobConfig();
        String urlString = "filezzz://path/to/zipFile";
        config.addJarsInZip(new URL(urlString));
    }

    @Test
    public void testAddZipOfJar_with_Path() throws Exception {
        JobConfig config = new JobConfig();
        String path = "/path/to/zipFile";
        config.addJarsInZip(path);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("zipFile", resourceConfig.getId());
        assertEquals(ResourceType.JARS_IN_ZIP, resourceConfig.getResourceType());
        assertEquals(new File(path).toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAddZipOfJar_with_File() throws Exception {
        JobConfig config = new JobConfig();
        File file = new File("/path/to/zipFile");
        config.addJarsInZip(file);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("zipFile", resourceConfig.getId());
        assertEquals(ResourceType.JARS_IN_ZIP, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAttachFile_with_Url() throws Exception {
        JobConfig config = new JobConfig();
        String urlString = "file://path/to/resourceFile";
        config.attachFile(new URL(urlString));
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("resourceFile", resourceConfig.getId());
        assertEquals(ResourceType.FILE, resourceConfig.getResourceType());
        assertEquals(urlString, resourceConfig.getUrl().toString());
    }

    @Test
    public void testAttachFile_with_MalformedUrl_throws_Exception() throws Exception {
        expectedException.expect(MalformedURLException.class);
        JobConfig config = new JobConfig();
        String urlString = "filezzz://path/to/file";
        config.attachFile(new URL(urlString));
    }

    @Test
    public void testAttachFile_with_Url_and_ResourceName() throws Exception {
        JobConfig config = new JobConfig();
        String resourceName = "resourceFileName";
        String urlString = "file://path/to/resourceFile";
        config.attachFile(new URL(urlString), resourceName);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(resourceName, resourceConfig.getId());
        assertEquals(ResourceType.FILE, resourceConfig.getResourceType());
        assertEquals(urlString, resourceConfig.getUrl().toString());
    }

    @Test
    public void testAttachFile_with_Path() throws Exception {
        JobConfig config = new JobConfig();
        String path = "/path/to/resource";
        config.attachFile(path);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("resource", resourceConfig.getId());
        assertEquals(ResourceType.FILE, resourceConfig.getResourceType());
        assertEquals(new File(path).toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAttachFile_with_Path_and_ResourceName() throws Exception {
        JobConfig config = new JobConfig();
        String resourceName = "resourceFileName";
        String path = "/path/to/jarfile";
        config.attachFile(path, resourceName);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(resourceName, resourceConfig.getId());
        assertEquals(ResourceType.FILE, resourceConfig.getResourceType());
        assertEquals(new File(path).toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAttachFile_with_File() throws Exception {
        JobConfig config = new JobConfig();
        File file = new File("/path/to/resource");
        config.attachFile(file);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("resource", resourceConfig.getId());
        assertEquals(ResourceType.FILE, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAttachFile_with_File_and_ResourceName() throws Exception {
        JobConfig config = new JobConfig();
        String resourceName = "resourceFileName";
        File file = new File("/path/to/resource");
        config.attachFile(file, resourceName);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(resourceName, resourceConfig.getId());
        assertEquals(ResourceType.FILE, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAttachFile_with_DuplicateId_throws_Exception() throws Exception {
        JobConfig config = new JobConfig();
        String id = "resourceFileName";
        File file = new File("/path/to/resource");

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(id);

        config.attachFile(file, id);
        config.attachFile(file, id);
    }

    @Test
    public void testAttachDirectory_with_Url() throws Exception {
        JobConfig config = new JobConfig();
        String urlString = "file://path/to/resourceFile";
        config.attachDirectory(new URL(urlString));
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("resourceFile", resourceConfig.getId());
        assertEquals(ResourceType.DIRECTORY, resourceConfig.getResourceType());
        assertEquals(urlString, resourceConfig.getUrl().toString());
    }

    @Test
    public void testAttachDirectory_with_MalformedUrl_throws_Exception() throws Exception {
        expectedException.expect(MalformedURLException.class);
        JobConfig config = new JobConfig();
        String urlString = "filezzz://path/to/file";
        config.attachDirectory(new URL(urlString));
    }

    @Test
    public void testAttachDirectory_with_Url_and_ResourceName() throws Exception {
        JobConfig config = new JobConfig();
        String resourceName = "resourceFileName";
        String urlString = "file://path/to/resourceFile";
        config.attachDirectory(new URL(urlString), resourceName);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(resourceName, resourceConfig.getId());
        assertEquals(ResourceType.DIRECTORY, resourceConfig.getResourceType());
        assertEquals(urlString, resourceConfig.getUrl().toString());
    }

    @Test
    public void testAttachDirectory_with_Path() throws Exception {
        JobConfig config = new JobConfig();
        String path = "/path/to/resource";
        config.attachDirectory(path);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("resource", resourceConfig.getId());
        assertEquals(ResourceType.DIRECTORY, resourceConfig.getResourceType());
        assertEquals(new File(path).toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAttachDirectory_with_Path_and_ResourceName() throws Exception {
        JobConfig config = new JobConfig();
        String resourceName = "resourceFileName";
        String path = "/path/to/jarfile";
        config.attachDirectory(path, resourceName);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(resourceName, resourceConfig.getId());
        assertEquals(ResourceType.DIRECTORY, resourceConfig.getResourceType());
        assertEquals(new File(path).toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAttachDirectory_with_File() throws Exception {
        JobConfig config = new JobConfig();
        File file = new File("/path/to/resource");
        config.attachDirectory(file);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("resource", resourceConfig.getId());
        assertEquals(ResourceType.DIRECTORY, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAttachDirectory_with_File_and_ResourceName() throws Exception {
        JobConfig config = new JobConfig();
        String resourceName = "resourceFileName";
        File file = new File("/path/to/resource");
        config.attachDirectory(file, resourceName);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(resourceName, resourceConfig.getId());
        assertEquals(ResourceType.DIRECTORY, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAttachDirectory_with_DuplicateId_throws_Exception() throws Exception {
        JobConfig config = new JobConfig();
        String id = "dirName";
        File file = new File("/path/to/dirName");

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(id);

        config.attachDirectory(file, id);
        config.attachDirectory(file, id);
    }

}
