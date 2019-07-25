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
import com.hazelcast.spi.Operation;

import java.io.Serializable;

import static com.hazelcast.jet.Util.idToString;

// TODO [viliam] serialization
// TODO [viliam] use AsyncOperation (liveOpRegistry)
public class LightJobOperation extends Operation implements Serializable {

    private final DAG dag;
    private long jobId;

    public LightJobOperation(DAG dag) {
        this.dag = dag;
    }

    protected JetService getJetService() {
        assert getServiceName().equals(JetService.SERVICE_NAME) : "Service is not JetService";
        return getService();
    }

    @Override
    public void run() {
        if (!getNodeEngine().getClusterService().isMaster()) {
            throw new JetException("Cannot submit job " + idToString(jobId) + " to non-master node. Master address: "
                    + getNodeEngine().getClusterService().getMasterAddress());
        }
        jobId = getJetService().getJobRepository().newJobId();
        new LightMasterContext(getNodeEngine(), dag, jobId)
                .start()
                .whenComplete((r, t) -> sendResponse(t != null ? t : r));
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }
}
