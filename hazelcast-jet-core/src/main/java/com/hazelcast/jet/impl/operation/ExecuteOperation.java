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

import java.util.concurrent.CompletionStage;

class ExecuteOperation extends AsyncExecutionOperation {

    private volatile CompletionStage<Void> executionFuture;

    ExecuteOperation(long executionId) {
        super(executionId);
    }

    private ExecuteOperation() {
        // for deserialization
    }

    @Override
    public void cancel() {
        if (executionFuture != null) {
            executionFuture.toCompletableFuture().cancel(true);
        }
    }

    @Override
    protected void doRun() throws Exception {
        JetService service = getService();
        getLogger().info("Start execution of plan for job " + executionId + " from caller " + getCallerAddress() + '.');
        executionFuture = service
                .getExecutionContext(executionId)
                .execute(f -> f.handle((r, error) -> error != null ? error : null)
                               .thenAccept((value) -> {
                                   getLogger().fine("Execution of plan for job " + executionId + " completed.");
                                   doSendResponse(value);
                               }));
    }
}
