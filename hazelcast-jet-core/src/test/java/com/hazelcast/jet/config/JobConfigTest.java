/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class JobConfigTest extends JetTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void when_setName_thenReturnsName() {
        // When
        JobConfig config = new JobConfig();
        String name = "myJobName";
        config.setName(name);

        // Then
        assertEquals(name, config.getName());
    }

    @Test
    public void when_enableSplitBrainProtection_thenReturnsEnabled() {
        // When
        JobConfig config = new JobConfig();
        config.setSplitBrainProtection(true);

        // Then
        assertTrue(config.isSplitBrainProtectionEnabled());
    }

    @Test
    public void when_enableAutoScaling_thenReturnsEnabled() {
        // When
        JobConfig config = new JobConfig();
        config.setAutoScaling(true);

        // Then
        assertTrue(config.isAutoScaling());
    }

    @Test
    public void when_setProcessingGuarantee_thenReturnsProcessingGuarantee() {
        // When
        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(EXACTLY_ONCE);

        // Then
        assertEquals(EXACTLY_ONCE, config.getProcessingGuarantee());
    }

    @Test
    public void when_setSnapshotIntervalMillis_thenReturnsSnapshotIntervalMillis() {
        // When
        JobConfig config = new JobConfig();
        config.setSnapshotIntervalMillis(50);

        // Then
        assertEquals(50, config.getSnapshotIntervalMillis());
    }

    @Test
    public void when_losslessRestartEnabled_then_openSourceMemberDoesNotStart() {
        // When
        JetConfig jetConfig = new JetConfig();
        jetConfig.getInstanceConfig().setLosslessRestartEnabled(true);

        // Then
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Hot Restart requires Hazelcast Enterprise Edition");
        createJetMember(jetConfig);
    }

    @Test
    public void when_registerSerializer() {
        // Given
        JobConfig config = new JobConfig();

        // When
        StreamSerializer<Object> serializer = new StreamSerializer<Object>() {

            @Override
            public int getTypeId() {
                return 0;
            }

            @Override
            public void write(ObjectDataOutput out, Object object) {
            }

            @Override
            public Object read(ObjectDataInput in) {
                return null;
            }

            @Override
            public void destroy() {
            }
        };
        Object object = new Object() {
        };

        config.registerSerializerFor(object.getClass(), serializer.getClass());

        // Then
        Map<String, String> serializerConfigs = config.getSerializerConfigs();
        assertThat(serializerConfigs.entrySet(), hasSize(1));
        assertThat(serializerConfigs.keySet(), contains(object.getClass().getName()));
        assertThat(serializerConfigs.values(), contains(serializer.getClass().getName()));
    }
}
