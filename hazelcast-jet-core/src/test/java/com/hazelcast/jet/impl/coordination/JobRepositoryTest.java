/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.coordination;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static com.hazelcast.jet.impl.util.JetGroupProperty.JOB_EXPIRATION_DURATION;
import static com.hazelcast.jet.impl.util.JetGroupProperty.JOB_SCAN_PERIOD;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class JobRepositoryTest extends JetTestSupport {

    private static final long JOB_EXPIRATION_TIME_IN_MILLIS = SECONDS.toMillis(1);
    private static final long JOB_SCAN_PERIOD_IN_MILLIS = HOURS.toMillis(1);

    private JetTestInstanceFactory factory = new JetTestInstanceFactory();
    private JobConfig jobConfig = new JobConfig();
    private JetInstance instance;
    private JobRepository jobRepository;

    @Before
    public void setup() {
        JetConfig config = new JetConfig();
        Properties properties = config.getProperties();
        properties.setProperty(JOB_EXPIRATION_DURATION.getName(), Long.toString(JOB_EXPIRATION_TIME_IN_MILLIS));
        properties.setProperty(JOB_SCAN_PERIOD.getName(), Long.toString(JOB_SCAN_PERIOD_IN_MILLIS));

        instance = factory.newMember(config);
        jobRepository = new JobRepository(instance);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void when_jobIsCompleted_then_expiredJobIsCleaned() {
        long jobIb = uploadResourcesForNewJob();
        DAG dag = new DAG();
        jobRepository.newJobRecord(jobIb, dag, jobConfig);

        assertFalse(jobRepository.getJobResources(jobIb).isEmpty());
        assertNotNull(jobRepository.getJob(jobIb));

        sleepUntilJobExpires();

        jobRepository.cleanup(singleton(jobIb), emptySet());

        assertNull(jobRepository.getJob(jobIb));
        assertTrue(jobRepository.getJobResources(jobIb).isEmpty());
    }

    @Test
    public void when_jobIsRunning_then_expiredJobIsNotCleared() {
        long jobIb = uploadResourcesForNewJob();
        DAG dag = new DAG();
        jobRepository.newJobRecord(jobIb, dag, jobConfig);

        assertFalse(jobRepository.getJobResources(jobIb).isEmpty());
        assertNotNull(jobRepository.getJob(jobIb));

        sleepUntilJobExpires();

        jobRepository.cleanup(emptySet(), singleton(jobIb));

        assertNotNull(jobRepository.getJob(jobIb));
        assertFalse(jobRepository.getJobResources(jobIb).isEmpty());
    }

    @Test
    public void when_jobExpires_then_jobIsCleared() {
        long jobIb = uploadResourcesForNewJob();

        DAG dag = new DAG();
        jobRepository.newJobRecord(jobIb, dag, jobConfig);

        assertFalse(jobRepository.getJobResources(jobIb).isEmpty());
        assertNotNull(jobRepository.getJob(jobIb));

        sleepUntilJobExpires();

        jobRepository.cleanup(emptySet(), emptySet());

        assertNull(jobRepository.getJob(jobIb));
        assertTrue(jobRepository.getJobResources(jobIb).isEmpty());
    }

    @Test
    public void when_onlyJobResourcesExist_then_theyAreClearedAfterTheyExpire() {
        long jobIb = uploadResourcesForNewJob();

        sleepUntilJobExpires();

        jobRepository.cleanup(emptySet(), emptySet());

        assertTrue(jobRepository.getJobResources(jobIb).isEmpty());
    }

    private long uploadResourcesForNewJob() {
        long jobIb = jobRepository.newId();
        jobConfig.addClass(DummyClass.class);
        jobRepository.uploadJobResources(jobIb, jobConfig);
        return jobIb;
    }

    private void sleepUntilJobExpires() {
        sleepAtLeastMillis(2 * JOB_EXPIRATION_TIME_IN_MILLIS);
    }


    static class DummyClass {

    }

}
