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

/**
 * Interface for all config builders.
 *
 * @see com.hazelcast.jet.impl.config.XmlJetConfigBuilder
 * @see com.hazelcast.jet.impl.config.YamlJetConfigBuilder
 */
public interface JetConfigBuilder {

    /**
     * Builds JetConfig object.
     *
     * @return Built JetConfig object
     */
    JetConfig build();
}
