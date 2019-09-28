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

package com.hazelcast.client.impl.protocol.template;

import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Request;
import com.hazelcast.client.impl.protocol.constants.ResponseMessageConst;
import com.hazelcast.nio.serialization.Data;

@GenerateCodec(id = TemplateConstants.JET_TEMPLATE_ID, name = "Jet", ns = "Jet")
public interface JetCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.VOID)
    void submitJob(long jobId, Data dag, Data jobConfig);

    @Request(id = 2, retryable = false, response = ResponseMessageConst.VOID)
    void terminateJob(long jobId, int terminateMode);

    @Request(id = 3, retryable = true, response = ResponseMessageConst.INTEGER)
    Object getJobStatus(long jobId);

    @Request(id = 4, retryable = true, response = ResponseMessageConst.LIST_LONG)
    Object getJobIds();

    // must not be retryable, Jet client has own retry mechanism here
    @Request(id = 5, retryable = false, response = ResponseMessageConst.VOID)
    void joinSubmittedJob(long jobId);

    @Request(id = 6, retryable = true, response = ResponseMessageConst.LIST_LONG)
    Object getJobIdsByName(String name);

    @Request(id = 7, retryable = true, response = ResponseMessageConst.LONG)
    long getJobSubmissionTime(long jobId);

    @Request(id = 8, retryable = true, response = ResponseMessageConst.DATA)
    Object getJobConfig(long jobId);

    @Request(id = 9, retryable = true, response = ResponseMessageConst.DATA)
    Object readMetrics(String uuid, long fromSequence);

    @Request(id = 10, retryable = false, response = ResponseMessageConst.VOID)
    void resumeJob(long jobId);

    @Request(id = 11, retryable = false, response = ResponseMessageConst.VOID)
    void exportSnapshot(long jobId, String name, boolean cancelJob);

    @Request(id = 12, retryable = true, response = ResponseMessageConst.DATA)
    Object getJobSummaryList();

    @Request(id = 13, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object existsDistributedObject(String serviceName, String objectName);

    @Request(id = 14, retryable = true, response = ResponseMessageConst.CLUSTER_METADATA)
    Object getClusterMetadata();

    @Request(id = 15, retryable = true, response = ResponseMessageConst.STRING)
    Object getMemberXmlConfiguration();

    @Request(id = 16, retryable = true, response = ResponseMessageConst.DATA)
    Object getJobMetrics(long jobId);

}
