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
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;

public class SubmitJobOperation extends AsyncJobOperation {

    // force serialization of fields to avoid sharing of the mutable instances if submitted to the master member
    private Data dag;
    private Data config;

    public SubmitJobOperation() {
    }

    public SubmitJobOperation(long jobId, Data dag, Data config) {
        super(jobId);
        this.dag = dag;
        this.config = config;
    }

    @Override
    public void doRun() {
        JetService service = getService();
        JobCoordinationService coordinationService = service.getJobCoordinationService();
        coordinationService.submitJob(jobId(), dag, config)
                           .whenComplete(withTryCatch(getLogger(), (r, f) -> sendResponse(f != null ? peel(f) : r)));
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.SUBMIT_JOB_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(dag);
        out.writeData(config);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        dag = in.readData();
        config = in.readData();
    }
}
