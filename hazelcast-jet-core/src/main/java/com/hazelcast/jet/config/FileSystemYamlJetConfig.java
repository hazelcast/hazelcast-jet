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

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.jet.impl.config.YamlJetConfigBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * A {@link JetConfig} which includes functionality for loading itself from a
 * Yaml configuration file.
 *
 * @see ClasspathYamlJetConfig
 * @see UrlYamlJetConfig
 * @see InMemoryYamlJetConfig
 */
public class FileSystemYamlJetConfig extends JetConfig {

    private static final ILogger LOGGER = Logger.getLogger(FileSystemYamlJetConfig.class);

    /**
     * Creates a JetConfig based on a Hazelcast Jet Yaml file and uses the
     * System.properties to resolve variables in the Yaml.
     *
     * @param configFilename the path of the Hazelcast Jet Yaml configuration file
     * @throws NullPointerException          if configFilename is {@code null}
     * @throws FileNotFoundException         if the file is not found
     * @throws InvalidConfigurationException if the Yaml content is invalid
     */
    public FileSystemYamlJetConfig(String configFilename) throws FileNotFoundException {
        this(configFilename, System.getProperties());
    }

    /**
     * Creates a JetConfig based on a Hazelcast Jet Yaml file.
     *
     * @param configFilename the path of the Hazelcast Jet Yaml configuration file
     * @param properties     the Properties to resolve variables in the Yaml
     * @throws FileNotFoundException         if the file is not found
     * @throws NullPointerException          if configFilename is {@code null}
     * @throws IllegalArgumentException      if properties is {@code null}
     * @throws InvalidConfigurationException if the Yaml content is invalid
     */
    public FileSystemYamlJetConfig(String configFilename, Properties properties) throws FileNotFoundException {
        this(new File(configFilename), properties);
    }

    /**
     * Creates a JetConfig based on a Hazelcast Jet Yaml file and uses the
     * System.properties to resolve variables in the Yaml.
     *
     * @param configFile the path of the Hazelcast Jet Yaml configuration file
     * @throws FileNotFoundException         if the file doesn't exist
     * @throws InvalidConfigurationException if the Yaml content is invalid
     */
    public FileSystemYamlJetConfig(File configFile) throws FileNotFoundException {
        this(configFile, System.getProperties());
    }

    /**
     * Creates a JetConfig based on a Hazelcast Jet Yaml file.
     *
     * @param configFile the path of the Hazelcast Jet Yaml configuration file
     * @param properties the Properties to resolve variables in the Yaml
     * @throws IllegalArgumentException      if configFile or properties is {@code null}
     * @throws FileNotFoundException         if the file doesn't exist
     * @throws InvalidConfigurationException if the Yaml content is invalid
     */
    public FileSystemYamlJetConfig(File configFile, Properties properties) throws FileNotFoundException {
        checkTrue(configFile != null, "configFile can't be null");
        checkTrue(properties != null, "properties can't be null");

        LOGGER.info("Configuring Hazelcast Jet from '" + configFile.getAbsolutePath() + "'.");
        InputStream in = new FileInputStream(configFile);
        new YamlJetConfigBuilder(in).setProperties(properties).build(this);
    }
}
