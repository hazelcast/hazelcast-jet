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
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

/**
 * A {@link JetConfig} which is initialized by loading the XML configuration
 * files from the classpath.
 *
 * @see FileSystemXmlJetConfig
 */
public class ClasspathXmlJetConfig extends JetConfig {

    private static final ILogger LOGGER = Logger.getLogger(ClasspathXmlJetConfig.class);

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
    public ClasspathXmlJetConfig(@Nullable String jetConfigResource,
                                 @Nullable String memberConfigResource) {
        this(jetConfigResource, memberConfigResource, System.getProperties());
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
    public ClasspathXmlJetConfig(@Nullable String jetConfigResource,
                                 @Nullable String memberConfigResource,
                                 Properties properties) {
        this(Thread.currentThread().getContextClassLoader(), jetConfigResource, memberConfigResource, properties);
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
    public ClasspathXmlJetConfig(ClassLoader classLoader,
                                 @Nullable String jetConfigResource,
                                 @Nullable String memberConfigResource) {
        this(classLoader, jetConfigResource, memberConfigResource, System.getProperties());
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
    public ClasspathXmlJetConfig(ClassLoader classLoader,
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

        XmlJetConfigBuilder.loadConfig(properties, jetStream, memberStream, this);
    }
}
