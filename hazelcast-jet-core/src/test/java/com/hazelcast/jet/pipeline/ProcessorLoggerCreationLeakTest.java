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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.test.SerialTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;

@Category(SerialTest.class)
public class ProcessorLoggerCreationLeakTest extends JetTestSupport {

    private static final String HAZELCAST_LOGGING_TYPE = "hazelcast.logging.type";
    private static final String HAZELCAST_LOGGING_CLASS = "hazelcast.logging.class";

    private String prevLoggingType;
    private String prevLoggingClass;
    private JetInstance jet;

    @Before
    public void setup() {
        prevLoggingType = System.getProperty(HAZELCAST_LOGGING_TYPE);
        prevLoggingClass = System.getProperty(HAZELCAST_LOGGING_CLASS);

        System.clearProperty(HAZELCAST_LOGGING_TYPE);
        System.setProperty(HAZELCAST_LOGGING_CLASS, MockLoggingFactory.class.getCanonicalName());

        jet = createJetMember();
    }

    @After
    public void after() {
        if (prevLoggingType == null) {
            System.clearProperty(HAZELCAST_LOGGING_TYPE);
        } else {
            System.setProperty(HAZELCAST_LOGGING_TYPE, prevLoggingType);
        }
        if (prevLoggingClass == null) {
            System.clearProperty(HAZELCAST_LOGGING_CLASS);
        } else {
            System.setProperty(HAZELCAST_LOGGING_CLASS, prevLoggingClass);
        }
    }

    @Test
    public void submitJobs_whenFinished_thenClearLogger() {
        String jobName = randomString();
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1, 2, 3, 4))
                .writeTo(Sinks.noop());

        for (int i = 0; i < 10; i++) {
            JobConfig jobConfig = new JobConfig().setName(jobName + i);
            jet.newJob(p, jobConfig).join();
        }
        assertFalse(MockLoggingFactory.loggersMap.keySet().stream().anyMatch(name -> name.contains(jobName)));
    }

}
