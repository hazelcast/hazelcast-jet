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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.util.Preconditions.checkTrue;
import static com.hazelcast.util.StringUtil.isNullOrEmptyAfterTrim;
import static com.hazelcast.util.StringUtil.stringToBytes;

/**
 * A {@link JetConfig} which includes functionality for loading itself from a
 * in-memory YAML String.
 *
 * @see ClasspathYamlJetConfig
 * @see FileSystemYamlJetConfig
 * @see UrlYamlJetConfig
 */
public class InMemoryYamlJetConfig extends JetConfig {

    private static final ILogger LOGGER = Logger.getLogger(InMemoryYamlJetConfig.class);

    /**
     * Creates a Config from the provided YAML string and uses the System.properties to resolve variables
     * in the YAML.
     *
     * @param yaml the YAML content as a Hazelcast Jet YAML String
     * @throws IllegalArgumentException      if the YAML is null or empty
     * @throws InvalidConfigurationException if the YAML content is invalid
     */
    public InMemoryYamlJetConfig(String yaml) {
        this(yaml, System.getProperties());
    }

    /**
     * Creates a Config from the provided YAML string and properties to resolve the variables in the YAML.
     *
     * @param yaml the YAML content as a Hazelcast Jet YAML String
     * @throws IllegalArgumentException      if the YAML is null or empty or if properties is null
     * @throws InvalidConfigurationException if the YAML content is invalid
     */
    public InMemoryYamlJetConfig(String yaml, Properties properties) {
        LOGGER.info("Configuring Hazelcast Jet from 'in-memory YAML'.");
        if (isNullOrEmptyAfterTrim(yaml)) {
            throw new IllegalArgumentException("YAML configuration is null or empty! Please use a well-structured YAML.");
        }
        checkTrue(properties != null, "properties can't be null");

        InputStream in = new ByteArrayInputStream(stringToBytes(yaml));
        new YamlJetConfigBuilder(in).setProperties(properties).build(this);
    }
}
