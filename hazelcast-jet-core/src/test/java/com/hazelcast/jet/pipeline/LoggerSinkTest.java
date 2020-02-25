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
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.test.SerialTest;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category(SerialTest.class)
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class LoggerSinkTest extends JetTestSupport {

    @Mock
    Appender appender;

    @Captor
    ArgumentCaptor<LogEvent> logCaptor;

    @Before
    public void setup() {
        when(appender.getName()).thenReturn("mock");
        when(appender.isStarted()).thenReturn(true);

        LoggerContext ctx = LoggerContext.getContext();
        Configuration cfg = ctx.getConfiguration();
        cfg.addAppender(appender);

        Logger logger = ctx.getRootLogger();

        assert logger.isInfoEnabled() : "info is enabled";

        logger.addAppender(appender);

        ctx.updateLoggers();
    }

    @Test
//    @Ignore("currently fails only on Jenkins")
    public void loggerSink() {
        // Given
        JetInstance jet = createJetMember();
        String srcName = randomName();

        jet.getList(srcName).add(0);

        Pipeline p = Pipeline.create();

        // When
        p.readFrom(Sources.<Integer>list(srcName))
         .map(i -> i + "-shouldBeSeenOnTheSystemOutput")
         .writeTo(Sinks.logger());

        jet.newJob(p).join();

        // Then
        verify(appender, atLeast(1)).append(logCaptor.capture());

        assertFalse(logCaptor.getAllValues().isEmpty());
        List<LogEvent> allValues = logCaptor.getAllValues();
        boolean match = allValues
                .stream()
                .map(LogEvent::getMessage)
                .anyMatch(message -> message.getFormattedMessage().contains("0-shouldBeSeenOnTheSystemOutput"));
        Assert.assertTrue(match);
    }

    @After
    public void teardown() {
        LoggerContext ctx = LoggerContext.getContext();
        ctx.getRootLogger().removeAppender(appender);
        ctx.updateLoggers();
    }
}
