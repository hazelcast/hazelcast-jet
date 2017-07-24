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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;

public class JoinJobOperation extends AsyncExecutionOperation implements IdentifiedDataSerializable {

    private volatile CompletableFuture<Throwable> executionFuture;

    public JoinJobOperation() {
    }

    public JoinJobOperation(long jobId) {
        super(jobId);
    }

    @Override
    protected void doRun() {
        JetService service = getService();
        executionFuture = service.startOrJoinJob(jobId);
        executionFuture.whenComplete((t1, t2) -> {
            Throwable response = t1 != null ? t1 : t2;
            doSendResponse(peel(response));
        });
    }

    @Override
    public void cancel() {
        if (executionFuture != null) {
            executionFuture.cancel(true);
        }
    }

    @Override
    public int getId() {
        return JetImplDataSerializerHook.JOIN_JOB_OP;
    }

}
