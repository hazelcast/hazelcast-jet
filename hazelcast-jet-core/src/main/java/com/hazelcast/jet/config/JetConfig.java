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

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration object for a Jet instance.
 */
public class JetConfig {

    /**
     * The default port number for the cluster auto-discovery mechanism's
     * multicast communication.
     */
    public static final int DEFAULT_JET_MULTICAST_PORT = 54326;

    private static final ILogger LOGGER = Logger.getLogger(JetConfig.class);

    private Config hazelcastConfig = defaultHazelcastConfig();
    private InstanceConfig instanceConfig = new InstanceConfig();
    private EdgeConfig defaultEdgeConfig = new EdgeConfig();
    private Properties properties = new Properties();

    /**
     * Creates a new JetConfig with the default configuration. Doesn't
     * consider any files.
     */
    public JetConfig() {
    }

    /**
     * Loads JetConfig from the default location. Also loads the nested
     * {@linkplain #getHazelcastConfig() Hazelcast config} from its default
     * location. It will use {@code System.getProperties()} to resolve the
     * variables in the XML.
     */
    public static JetConfig loadDefault() {
        return XmlJetConfigBuilder.loadConfig(null, null);
    }

    /**
     * Loads JetConfig from the default location. Also loads the nested
     * {@linkplain #getHazelcastConfig() Hazelcast config} from its default
     * location. It will use the given {@code properties} to resolve the
     * variables in the XML.
     */
    public static JetConfig loadDefault(@Nonnull Properties properties) {
        return XmlJetConfigBuilder.loadConfig(null, properties);
    }

    /**
     * Loads JetConfig from the classpath resource named by the argument.
     * Uses the thread's context class loader. Uses {@code
     * System.getProperties()} to resolve the variables in the XML.
     * <p>
     * This method loads the nested {@linkplain #getHazelcastConfig() Hazelcast
     * config} from its default location, but you can replace it by calling
     * {@link #setHazelcastConfig setHazelcastConfig()} using, for example,
     * {@link com.hazelcast.config.ClasspathXmlConfig ClasspathXmlConfig} or
     * {@link com.hazelcast.config.FileSystemXmlConfig FileSystemXmlConfig}.
     *
     * @param resource names the classpath resource containing the XML configuration file
     *
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     * @throws IllegalArgumentException if classpath resource is not found
     */
    public static JetConfig loadFromClasspath(@Nonnull String resource) {
        return loadFromClasspath(resource, System.getProperties());
    }

    /**
     * Creates a JetConfig which is loaded from classpath resource using the
     * thread's context class loader. It will use the given {@code properties}
     * to resolve variables in the XML.
     * <p>
     * The returned JetConfig will refer to {@linkplain #getHazelcastConfig()
     * Hazelcast config} that will be loaded from the default location. You can
     * replace it by calling {@link #setHazelcastConfig} using, for example,
     * {@link com.hazelcast.config.ClasspathXmlConfig} or {@link
     * com.hazelcast.config.FileSystemXmlConfig}.
     *
     * @param resource the classpath resource, an XML configuration file on the
     *      classpath
     *
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     * @throws IllegalArgumentException if classpath resource is not found
     */
    public static JetConfig loadFromClasspath(@Nonnull String resource, @Nonnull Properties properties) {
        LOGGER.info("Configuring Hazelcast Jet from '" + resource + "' on classpath");
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
        if (stream == null) {
            throw new IllegalArgumentException("Specified resource '" + resource + "' cannot be found on classpath");
        }
        return loadFromStream(stream, properties);
    }

    /**
     * Creates a JetConfig by reading the supplied stream. It will use {@code
     * System.properties} to resolve variables in the XML.
     * <p>
     * The returned JetConfig will refer to {@linkplain #getHazelcastConfig()
     * Hazelcast config} that will be loaded from the default location. You can
     * replace it by calling {@link #setHazelcastConfig} using, for example,
     * {@link com.hazelcast.config.ClasspathXmlConfig} or {@link
     * com.hazelcast.config.FileSystemXmlConfig}.

     *
     * @param configStream the InputStream to load the config from
     *
     * @throws IOException if an IO exception occurs
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public static JetConfig loadFromStream(@Nonnull InputStream configStream) {
        return loadFromStream(configStream, System.getProperties());
    }

    /**
     * Creates a JetConfig by reading the supplied stream. It will use the
     * given {@code properties} to resolve variables in the XML.
     * <p>
     * The returned JetConfig will refer to {@linkplain #getHazelcastConfig()
     * Hazelcast config} that will be loaded from the default location. You can
     * replace it by calling {@link #setHazelcastConfig} using, for example,
     * {@link com.hazelcast.config.ClasspathXmlConfig} or {@link
     * com.hazelcast.config.FileSystemXmlConfig}.

     *
     * @param configStream the InputStream to load the config from
     * @param properties the properties to resolve variables in the XML
     *
     * @throws IOException if an IO exception occurs
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public static JetConfig loadFromStream(@Nonnull InputStream configStream, @Nonnull Properties properties) {
        return XmlJetConfigBuilder.loadConfig(configStream, properties);
    }

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
}
