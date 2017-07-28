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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.init.JetImplDataSerializerHook;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

import static com.hazelcast.jet.impl.util.Util.idToString;

public class DoSnapshotOperation extends AsyncExecutionOperation {

    private long executionId;
    private long snapshotId;
    private volatile CompletionStage<Void> executionFuture;

    // for deserialization
    public DoSnapshotOperation() {
    }

    public DoSnapshotOperation(long jobId, long executionId, long snapshotId) {
        super(jobId);
        this.executionId = executionId;
        this.snapshotId = snapshotId;
    }

    @Override
    protected void doRun() throws Exception {
        JetService service = getService();

        executionFuture = service.doSnapshotOnMember(getCallerAddress(), jobId, executionId, snapshotId)
                .whenComplete((r, v) -> {
                    LoggingUtil.logFine(getLogger(), "Snapshot %d for job %s finished on member",
                            snapshotId, idToString(jobId));
                    doSendResponse(null);
                });
    }

    @Override
    public void cancel() {
        if (executionFuture != null) {
            executionFuture.toCompletableFuture().cancel(true);
        }
    }

    @Override
    public int getId() {
        return JetImplDataSerializerHook.DO_SNAPSHOT_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(executionId);
        out.writeLong(snapshotId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        executionId = in.readLong();
        snapshotId = in.readLong();
    }
}
