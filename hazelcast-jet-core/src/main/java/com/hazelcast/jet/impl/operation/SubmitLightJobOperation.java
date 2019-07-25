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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.Util.idToString;

public class SubmitLightJobOperation extends AsyncOperation {

    private DAG dag;

    public SubmitLightJobOperation() {
    }

    public SubmitLightJobOperation(DAG dag) {
        this.dag = dag;
    }

    protected JetService getJetService() {
        assert getServiceName().equals(JetService.SERVICE_NAME) : "Service is not JetService";
        return getService();
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        long jobId = getJetService().getJobRepository().newJobId();
        if (!getNodeEngine().getClusterService().isMaster()) {
            throw new JetException("Cannot submit job " + idToString(jobId) + " to non-master node. Master address: "
                    + getNodeEngine().getClusterService().getMasterAddress());
        }
        return new LightMasterContext(getNodeEngine(), dag, jobId)
                .start();
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.SUBMIT_LIGHT_JOB_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(dag);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        dag = in.readObject();
    }
}
