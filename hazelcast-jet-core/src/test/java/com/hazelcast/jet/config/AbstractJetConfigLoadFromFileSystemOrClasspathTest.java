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

    @Test
    public abstract void when_fileSystemFileSpecified_usesSpecifiedFile() throws IOException;

    @Test
    public abstract void when_classpathSpecifiedWithClassloader_usesSpecifiedResource();

}
