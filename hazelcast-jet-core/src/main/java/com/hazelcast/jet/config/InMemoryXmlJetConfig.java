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
import com.hazelcast.jet.impl.config.XmlJetConfigBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.util.Preconditions.checkTrue;
import static com.hazelcast.util.StringUtil.isNullOrEmptyAfterTrim;
import static com.hazelcast.util.StringUtil.stringToBytes;

/**
 * A {@link JetConfig} which includes functionality for loading itself from an
 * in-memory XML string.
 *
 * @see ClasspathXmlJetConfig
 * @see FileSystemXmlJetConfig
 * @see InMemoryXmlJetConfig
 */
public class InMemoryXmlJetConfig extends JetConfig {

    private static final ILogger LOGGER = Logger.getLogger(InMemoryXmlJetConfig.class);

    /**
     * Creates a Config from the provided XML string and uses the System.properties to resolve variables
     * in the XML.
     *
     * @param xml the XML content as a Hazelcast Jet XML String
     * @throws IllegalArgumentException      if the XML is null or empty
     * @throws InvalidConfigurationException if the XML content is invalid
     */
    public InMemoryXmlJetConfig(String xml) {
        this(xml, System.getProperties());
    }

    /**
     * Creates a Config from the provided XML string and properties to resolve the variables in the XML.
     *
     * @param xml the XML content as a Hazelcast Jet XML String
     * @throws IllegalArgumentException      if the XML is null or empty or if properties is null
     * @throws InvalidConfigurationException if the XML content is invalid
     */
    public InMemoryXmlJetConfig(String xml, Properties properties) {
        LOGGER.info("Configuring Hazelcast Jet from 'in-memory xml'.");
        if (isNullOrEmptyAfterTrim(xml)) {
            throw new IllegalArgumentException("XML configuration is null or empty! Please use a well-structured xml.");
        }
        checkTrue(properties != null, "properties can't be null");

        InputStream in = new ByteArrayInputStream(stringToBytes(xml));
        new XmlJetConfigBuilder(in).setProperties(properties).build(this);
    }

}
