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

package com.hazelcast.jet.impl.metrics;

import com.hazelcast.core.Member;
import com.hazelcast.jet.impl.JobExecutionService;
import com.hazelcast.jet.impl.JobMetricsUtil;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.jet.impl.JobMetricsUtil.addPrefixToDescriptor;

/**
 * Internal publisher which notifies the {@link JobExecutionService} about
 * the latest metric values.
 */
public class JobMetricsPublisher implements MetricsPublisher {

    private final JobExecutionService jobExecutionService;
    private final String namePrefix;
    private final ILogger logger;
    private final Map<Long, RawJobMetrics> jobMetrics = new HashMap<>();
    private final Map<Long, BlobPublisher> executionIdToPublisher = new HashMap<>();

    JobMetricsPublisher(
            @Nonnull JobExecutionService jobExecutionService,
            @Nonnull Member member,
            @Nonnull ILogger logger
    ) {
        Objects.requireNonNull(jobExecutionService, "jobExecutionService");
        Objects.requireNonNull(member, "member");
        Objects.requireNonNull(logger, "logger");

        this.jobExecutionService = jobExecutionService;
        this.namePrefix = JobMetricsUtil.getMemberPrefix(member);
        this.logger = logger;
    }

    @Override
    public void publishLong(String name, long value) {
        BlobPublisher blobPublisher = getPublisher(name);
        if (blobPublisher != null) {
            blobPublisher.publishLong(addPrefixToDescriptor(name, namePrefix), value);
        }
    }

    @Override
    public void publishDouble(String name, double value) {
        BlobPublisher blobPublisher = getPublisher(name);
        if (blobPublisher != null) {
            blobPublisher.publishDouble(addPrefixToDescriptor(name, namePrefix), value);
        }
    }

    @Override
    public void whenComplete() {
        for (Iterator<BlobPublisher> it = executionIdToPublisher.values().iterator(); it.hasNext(); ) {
            BlobPublisher publisher = it.next();
            // remove publisher that didn't receive any metrics
            if (publisher.getCount() == 0) {
                it.remove();
            }
            publisher.whenComplete();
        }

        jobExecutionService.updateMetrics(jobMetrics);
        jobMetrics.clear();
    }

    @Override
    public String name() {
        return "Job Metrics Publisher";
    }

    private BlobPublisher getPublisher(String name) {
        Long executionId = JobMetricsUtil.getExecutionIdFromMetricDescriptor(name);
        return executionId != null ?
                executionIdToPublisher.computeIfAbsent(executionId,
                        ignored -> new BlobPublisher(logger,
                                (blob, ts) -> jobMetrics.put(executionId, RawJobMetrics.of(ts, blob))
                        ))
                : null;
    }
}
