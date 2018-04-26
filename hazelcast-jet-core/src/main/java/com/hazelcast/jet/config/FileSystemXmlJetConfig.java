/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.impl.config.XmlJetConfigBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * A {@link JetConfig} which loads itself from XML configuration files.
 */
public class FileSystemXmlJetConfig extends JetConfig {

    private static final ILogger LOGGER = Logger.getLogger(FileSystemXmlJetConfig.class);

    /**
     * Creates a JetConfig based on a Hazelcast Jet xml file and a Hazelcast
     * xml file and uses the System.properties to resolve variables in the
     * XMLs.
     *
     * @param jetConfigFileName the path of the Hazelcast Jet xml configuration
     *      file, optional
     * @param memberConfigFileName the path of the Hazelcast xml configuration
     *      file, optional
     *
     * @throws IOException if an IO exception occurs
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public FileSystemXmlJetConfig(@Nullable String jetConfigFileName, 
                                  @Nullable String memberConfigFileName) {
        this(jetConfigFileName, memberConfigFileName, System.getProperties());
    }

    /**
     * Creates a JetConfig based on a Hazelcast Jet xml file and a Hazelcast
     * xml file and uses the given {@code properties} to resolve variables in
     * the XMLs.
     *
     * @param jetConfigFileName the path of the Hazelcast Jet xml configuration
     *      file, optional
     * @param memberConfigFileName the path of the Hazelcast xml configuration
     *      file, optional
     * @param properties the properties to resolve variables in the XMLs
     *
     * @throws IOException if an IO exception occurs
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public FileSystemXmlJetConfig(@Nullable String jetConfigFileName,
                                  @Nullable String memberConfigFileName, 
                                  Properties properties) {
        this(maybeCreateFile(jetConfigFileName), maybeCreateFile(memberConfigFileName), properties);
    }

    private static File maybeCreateFile(@Nullable String fileName) {
        if (fileName == null) {
            return null;
        }
        return new File(fileName);
    }

    /**
     * Creates a JetConfig based on a Hazelcast Jet xml file and a Hazelcast
     * xml file and uses the System.properties to resolve variables in the
     * XMLs.
     *
     * @param jetConfigFile the Hazelcast Jet xml configuration file, optional
     * @param memberConfigFile the  Hazelcast xml configuration file, optional
     *
     * @throws IOException if an IO exception occurs
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public FileSystemXmlJetConfig(@Nullable File jetConfigFile,
                                  @Nullable File memberConfigFile) {
        this(jetConfigFile, memberConfigFile, System.getProperties());
    }

    /**
     * Creates a JetConfig based on a Hazelcast Jet xml file and a Hazelcast
     * xml file and uses the given {@code properties} to resolve variables in
     * the XMLs.
     *
     * @param jetConfigFile the Hazelcast Jet xml configuration file, optional
     * @param memberConfigFile the  Hazelcast xml configuration file, optional
     * @param properties the properties to resolve variables in the XMLs
     *
     * @throws IOException if an IO exception occurs
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public FileSystemXmlJetConfig(@Nullable File jetConfigFile,
                                  @Nullable File memberConfigFile, 
                                  Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("properties can't be null");
        }

        InputStream jetStream = null;
        InputStream memberStream = null;
        try {
            if (jetConfigFile != null) {
                LOGGER.info("Configuring Hazelcast Jet from '" + jetConfigFile.getAbsolutePath() + "'");
                jetStream = new FileInputStream(jetConfigFile);
            }
            if (memberConfigFile != null) {
                LOGGER.info("Configuring Hazelcast member from '" + memberConfigFile.getAbsolutePath() + "'");
                memberStream = new FileInputStream(memberConfigFile);
            }
        } catch (FileNotFoundException e) {
            throw sneakyThrow(e);
        }

        XmlJetConfigBuilder.loadConfig(properties, jetStream, memberStream, this);
    }
}
