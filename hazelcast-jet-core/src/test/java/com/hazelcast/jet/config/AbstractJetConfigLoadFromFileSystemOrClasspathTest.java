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

import com.hazelcast.core.HazelcastException;
import org.junit.Test;

import java.io.IOException;

/**
 * Abstract test class defining the common test cases for loading XML and YAML
 * based configuration files from classpath or filesystem.
 *
 * @see XmlJetConfigLoadFromFileSystemOrClasspathTest
 * @see YamlJetConfigLoadFromFileSystemOrClasspathTest
 */
public abstract class AbstractJetConfigLoadFromFileSystemOrClasspathTest {

    @Test(expected = HazelcastException.class)
    public abstract void when_fileSystemNullFile_thenThrowsException() throws Exception;

    @Test(expected = HazelcastException.class)
    public abstract void when_fileSystemNullProperties_thenThrowsException() throws Exception;

    @Test(expected = HazelcastException.class)
    public abstract void when_fileSystemPathSpecifiedNonExistingFile_thenThrowsException() throws Exception;

    @Test
    public abstract void when_fileSystemFileSpecified_usesSpecifiedFile() throws IOException;

    @Test
    public abstract void when_fileSystemPathSpecified_usesSpecifiedFile() throws IOException;

    @Test(expected = HazelcastException.class)
    public abstract void when_classpathSpecifiedNonExistingFile_thenThrowsException();

    @Test(expected = HazelcastException.class)
    public abstract void when_classPathNullResource_thenThrowsException() throws Exception;

    @Test(expected = HazelcastException.class)
    public abstract void when_classPathNullProperties_thenThrowsException() throws Exception;

    @Test(expected = HazelcastException.class)
    public abstract void when_classPathNullClassloader_thenThrowsException() throws Exception;

    @Test
    public abstract void when_classpathSpecified_usesSpecifiedResource();

    @Test
    public abstract void when_classpathSpecifiedWithClassloader_usesSpecifiedResource();

}
