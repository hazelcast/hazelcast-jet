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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.core.JobSuspensionCause;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nullable;
import java.io.IOException;

public class JobSuspensionCauseImpl implements JobSuspensionCause, IdentifiedDataSerializable {

    private static final JobSuspensionCauseImpl REQUESTED_BY_USER = new JobSuspensionCauseImpl(null);

    private Throwable error;

    public JobSuspensionCauseImpl() { //needed for deserialization
    }

    private JobSuspensionCauseImpl(Throwable error) {
        this.error = error;
    }

    @Override
    public boolean requestedByUser() {
        return error == null;
    }

    @Override
    public boolean dueToError() {
        return error != null;
    }

    @Nullable
    @Override
    public Throwable errorCause() {
        return error;
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.JOB_SUSPENSION_CAUSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(error);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        error = in.readObject();
    }

    static JobSuspensionCauseImpl causedBy(Throwable cause) {
        return cause == null ? REQUESTED_BY_USER : new JobSuspensionCauseImpl(cause);
    }
}
