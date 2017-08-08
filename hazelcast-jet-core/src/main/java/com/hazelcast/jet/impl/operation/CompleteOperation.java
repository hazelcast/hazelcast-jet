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
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.Operation;

import java.io.IOException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.isJobRestartRequired;
import static com.hazelcast.jet.impl.util.Util.idToString;
import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;

public class CompleteOperation extends Operation implements IdentifiedDataSerializable {

    private long executionId;
    private Throwable error;

    public CompleteOperation() {
    }

    public CompleteOperation(long executionId, Throwable error) {
        this.executionId = executionId;
        this.error = error;
    }

    @Override
    public void run() throws Exception {
        ILogger logger = getLogger();
        JetService service = getService();

        Address callerAddress = getCallerAddress();
        logger.fine("Completing execution " + idToString(executionId) + " from caller: " + callerAddress
                + " with " + error);

        Address masterAddress = getNodeEngine().getMasterAddress();
        if (!callerAddress.equals(masterAddress)) {
            throw new IllegalStateException("Caller " + callerAddress + " cannot complete execution of "
                    + idToString(executionId) + " because it is not master. Master is: " + masterAddress);
        }

        service.completeExecution(executionId, error);
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        return isJobRestartRequired(throwable) ? THROW_EXCEPTION : super.onInvocationException(throwable);
    }

    @Override
    public int getFactoryId() {
        return JetImplDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetImplDataSerializerHook.COMPLETE_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(executionId);
        out.writeObject(error);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        executionId = in.readLong();
        error = in.readObject();
    }
}
