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

import com.hazelcast.config.Config;
import com.hazelcast.jet.impl.config.XmlJetConfigBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * Configuration object for a Jet instance.
 */
public class JetConfig {

    private static final ILogger LOGGER = Logger.getLogger(JetConfig.class);

    /**
     * The default port number for the cluster auto-discovery mechanism's
     * multicast communication.
     */
    public static final int DEFAULT_JET_MULTICAST_PORT = 54326;

    private Config hazelcastConfig = defaultHazelcastConfig();
    private InstanceConfig instanceConfig = new InstanceConfig();
    private EdgeConfig defaultEdgeConfig = new EdgeConfig();
    private Properties properties = new Properties();

    /**
     * Returns the configuration object for the underlying Hazelcast instance.
     */
    public Config getHazelcastConfig() {
        return hazelcastConfig;
    }

    /**
     * Sets the underlying IMDG instance's configuration object.
     */
    public JetConfig setHazelcastConfig(Config config) {
        hazelcastConfig = config;
        return this;
    }

    /**
     * Returns the Jet instance config.
     */
    public InstanceConfig getInstanceConfig() {
        return instanceConfig;
    }

    /**
     * Sets the Jet instance config.
     */
    public JetConfig setInstanceConfig(InstanceConfig instanceConfig) {
        this.instanceConfig = instanceConfig;
        return this;
    }

    /**
     * Returns the Jet-specific configuration properties.
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the Jet-specific configuration properties.
     */
    public JetConfig setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Returns the default DAG edge configuration.
     */
    public EdgeConfig getDefaultEdgeConfig() {
        return defaultEdgeConfig;
    }

    /**
     * Sets the configuration object that specifies the defaults to use
     * for a DAG edge configuration.
     */
    public JetConfig setDefaultEdgeConfig(EdgeConfig defaultEdgeConfig) {
        this.defaultEdgeConfig = defaultEdgeConfig;
        return this;
    }

    private static Config defaultHazelcastConfig() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setMulticastPort(DEFAULT_JET_MULTICAST_PORT);
        config.getGroupConfig().setName("jet");
        return config;
    }

    /**
     * Creates a JetConfig which is loaded from classpath resources using the
     * thread's context class loader. The {@code System.properties} are used to
     * resolve variables in the XMLs.
     *
     * @param jetConfigResource the resource, an XML configuration file on the
     *      classpath, optional
     * @param memberConfigResource the resource, an XML configuration file on
     *      the classpath, optional
     *
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public static JetConfig loadFromClassPath(@Nullable String jetConfigResource,
                                              @Nullable String memberConfigResource) {
        return loadFromClassPath(jetConfigResource, memberConfigResource, System.getProperties());
    }

    /**
     * Creates a JetConfig which is loaded from classpath resources using the
     * thread's context class loader. The given {@code properties} are used to
     * resolve variables in the XMLs.
     *
     * @param jetConfigResource the resource, an XML configuration file on the
     *      classpath, optional
     * @param memberConfigResource the resource, an XML configuration file on
     *      the classpath, optional
     *
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public static JetConfig loadFromClassPath(@Nullable String jetConfigResource,
                                              @Nullable String memberConfigResource,
                                              Properties properties) {
        return loadFromClassPath(Thread.currentThread().getContextClassLoader(), jetConfigResource, memberConfigResource, properties);
    }

    /**
     * Creates a JetConfig which is loaded from classpath resources. The {@code
     * System.properties} are used to resolve variables in the XMLs.
     *
     * @param classLoader the ClassLoader used to load the resource
     * @param jetConfigResource the resource, an XML configuration file on the
     *      classpath, optional
     * @param memberConfigResource the resource, an XML configuration file on
     *      the classpath, optional
     *
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public static JetConfig loadFromClassPath(ClassLoader classLoader,
                                              @Nullable String jetConfigResource,
                                              @Nullable String memberConfigResource) {
        return loadFromClassPath(classLoader, jetConfigResource, memberConfigResource, System.getProperties());
    }

    /**
     * Creates a JetConfig which is loaded from classpath resources. The given
     * {@code properties} are used to resolve variables in the XMLs.
     *
     * @param classLoader the ClassLoader used to load the resource
     * @param jetConfigResource the resource, an XML configuration file on the
     *      classpath, optional
     * @param memberConfigResource the resource, an XML configuration file on
     *      the classpath, optional
     * @param properties  the properties used to resolve variables in the XML
     *
     * @throws IllegalArgumentException              if classLoader or resource is {@code null}, or if the resource is not found
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public static JetConfig loadFromClassPath(ClassLoader classLoader,
                                              @Nullable String jetConfigResource,
                                              @Nullable String memberConfigResource,
                                              Properties properties) {
        Objects.requireNonNull(classLoader, "classLoader can't be null");
        Objects.requireNonNull(properties, "properties can't be null");

        InputStream jetStream = null;
        InputStream memberStream = null;
        if (jetConfigResource != null) {
            LOGGER.info("Configuring Hazelcast Jet from '" + jetConfigResource + "'");
            jetStream = classLoader.getResourceAsStream(jetConfigResource);
            if (jetStream == null) {
                throw new IllegalArgumentException("Specified resource '" + jetConfigResource + "' cannot be found");
            }
        }
        if (memberConfigResource != null) {
            LOGGER.info("Configuring Hazelcast member from '" + memberConfigResource + "'");
            memberStream = classLoader.getResourceAsStream(memberConfigResource);
            if (memberStream == null) {
                throw new IllegalArgumentException("Specified resource '" + memberConfigResource + "' cannot be found");
            }
        }

        return XmlJetConfigBuilder.loadConfig(properties, jetStream, memberStream);
    }

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
    public static JetConfig loadFromFile(@Nullable String jetConfigFileName,
                                  @Nullable String memberConfigFileName) {
        return loadFromFile(jetConfigFileName, memberConfigFileName, System.getProperties());
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
    public static JetConfig loadFromFile(@Nullable String jetConfigFileName,
                                  @Nullable String memberConfigFileName,
                                  Properties properties) {
        return loadFromFile(maybeCreateFile(jetConfigFileName), maybeCreateFile(memberConfigFileName), properties);
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
    public static JetConfig loadFromFile(@Nullable File jetConfigFile,
                                  @Nullable File memberConfigFile) {
        return loadFromFile(jetConfigFile, memberConfigFile, System.getProperties());
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
    public static JetConfig loadFromFile(@Nullable File jetConfigFile,
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

        return XmlJetConfigBuilder.loadConfig(properties, jetStream, memberStream);
    }
}
