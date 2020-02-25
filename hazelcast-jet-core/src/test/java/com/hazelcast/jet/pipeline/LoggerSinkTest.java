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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.test.SerialTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTest.class)
public class LoggerSinkTest extends JetTestSupport {

    private String prevLoggingType;

    @Before
    public void setup() {
        prevLoggingType = System.getProperty("hazelcast.logging.type");
        System.clearProperty("hazelcast.logging.type");
        System.setProperty("hazelcast.logging.class", MockLoggingFactory.class.getCanonicalName());
    }

    @Test
    public void loggerSink() {
        // Given
        JetConfig jetConfig = new JetConfig();
        JetInstance jet = createJetMember(jetConfig);
        String srcName = randomName();

        jet.getList(srcName).add(0);

        Pipeline p = Pipeline.create();

        // When
        p.readFrom(Sources.<Integer>list(srcName))
         .map(i -> i + "-shouldBeSeenOnTheSystemOutput")
         .writeTo(Sinks.logger());

        jet.newJob(p).join();

        // Then
        Assert.assertTrue("no message containing '0-shouldBeSeenOnTheSystemOutput' was found",
                MockLoggingFactory.capturedMessages
                        .stream()
                        .anyMatch(message -> message.contains("0-shouldBeSeenOnTheSystemOutput"))
        );
    }

    @After
    public void after() {
        System.clearProperty("hazelcast.logging.class");
        System.setProperty("hazelcast.logging.type", prevLoggingType);
    }

}
