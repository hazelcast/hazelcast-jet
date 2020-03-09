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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.jet.impl.config.ConfigProvider;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
public class ConfigProviderPriorityTest {

    @Test
    public void when_loadingJetConfigFromWorkDir_yamlAndXmlFilesArePresent_then_pickYaml() {
        JetConfig jetConfig = ConfigProvider.locateAndGetJetConfig();
        Assert.assertEquals("bar-yaml", jetConfig.getProperties().getProperty("foo"));
    }

    @Test
    public void when_loadingClientConfigFromWorkDir_yamlAndXmlFilesArePresent_then_pickYaml() {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        Assert.assertEquals("bar-yaml", clientConfig.getProperties().getProperty("foo"));
    }

    @Test
    public void when_loadingMemberConfigFromWorkDir_yamlAndXmlFilesArePresent_then_pickYaml() {
        Config config = ConfigProvider.locateAndGetMemberConfig(null);
        Assert.assertEquals("bar-yaml", config.getProperties().getProperty("foo"));
    }
}
