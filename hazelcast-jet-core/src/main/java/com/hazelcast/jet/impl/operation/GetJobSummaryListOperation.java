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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.JobCoordinationService;
import com.hazelcast.jet.impl.JobSummary;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.ReadonlyOperation;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class GetJobSummaryListOperation
        extends AsyncOperation
        implements IdentifiedDataSerializable, ReadonlyOperation {

    public GetJobSummaryListOperation() {
    }

    @Override
    public CompletableFuture<List<JobSummary>> doRun() {
        JetService service = getService();
        JobCoordinationService coordinationService = service.getJobCoordinationService();
        return coordinationService.getJobSummaryList();
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.GET_JOB_SUMMARY_LIST_OP;
    }
}
