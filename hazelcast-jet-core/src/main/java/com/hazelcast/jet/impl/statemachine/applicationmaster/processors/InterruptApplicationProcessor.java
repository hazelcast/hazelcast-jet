/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.statemachine.applicationmaster.processors;

import com.hazelcast.jet.impl.Dummy;
import com.hazelcast.jet.impl.container.ApplicationMaster;
import com.hazelcast.jet.impl.container.ContainerPayloadProcessor;
import com.hazelcast.jet.impl.container.ProcessingContainer;
import com.hazelcast.jet.impl.statemachine.container.requests.ContainerInterruptRequest;

import java.util.concurrent.TimeUnit;

public class InterruptApplicationProcessor implements ContainerPayloadProcessor<Dummy> {
    private final int secondToAwait;
    private final ApplicationMaster applicationMaster;

    public InterruptApplicationProcessor(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
        this.secondToAwait = this.applicationMaster.getApplicationContext().getApplicationConfig().getSecondsToAwait();
    }

    @Override
    public void process(Dummy payload) throws Exception {
        this.applicationMaster.registerInterruption();

        for (ProcessingContainer container : this.applicationMaster.containers()) {
            container.handleContainerRequest(new ContainerInterruptRequest()).get(this.secondToAwait, TimeUnit.SECONDS);
        }
    }
}
