/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.instance.JetBuildInfo;
import com.hazelcast.instance.Node;
import com.hazelcast.jet.impl.operation.PrepareForPassiveClusterOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

class NodeExtensionCommon {
    private static final String JET_LOGO =
            "\to   o   o   o---o o---o o     o---o   o   o---o o-o-o        o o---o o-o-o\n" +
            "\t|   |  / \\     /  |     |     |      / \\  |       |          | |       |\n" +
            "\to---o o---o   o   o-o   |     o     o---o o---o   |          | o-o     |\n" +
            "\t|   | |   |  /    |     |     |     |   |     |   |      \\   | |       |\n" +
            "\to   o o   o o---o o---o o---o o---o o   o o---o   o       o--o o---o   o";
    private static final String COPYRIGHT_LINE = "Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.";

    private final Node node;
    private JetService jetService;

    NodeExtensionCommon(Node node) {
        this.node = node;
    }

    void afterStart() {
        jetService = node.nodeEngine.getService(JetService.SERVICE_NAME);
        jetService.getJobCoordinationService().startScanningForJobs();
    }

    void beforeClusterStateChange(ClusterState requestedState) {
        if (requestedState != PASSIVE) {
            return;
        }
        jetService.getJobCoordinationService().shutdown();
        NodeEngineImpl ne = node.nodeEngine;
        try {
            ne.getOperationService().createInvocationBuilder(JetService.SERVICE_NAME,
                    new PrepareForPassiveClusterOperation(), ne.getMasterAddress())
              .invoke().get();
        } catch (InterruptedException | ExecutionException e) {
            throw rethrow(e);
        }
    }

    void onClusterStateChange(ClusterState newState) {
        if (jetService != null) {
            jetService.getJobCoordinationService().clusterChangeDone();
        }
    }

    void printNodeInfo(ILogger log, String addToProductName) {
        log.info(versionAndAddressMessage(addToProductName));
        log.fine(serializationVersionMessage());
        log.info('\n' + JET_LOGO);
        log.info(COPYRIGHT_LINE);
    }

    private String versionAndAddressMessage(@Nonnull String addToName) {
        JetBuildInfo jetBuildInfo = node.getBuildInfo().getJetBuildInfo();
        String build = jetBuildInfo.getBuild();
        String revision = jetBuildInfo.getRevision();
        if (!revision.isEmpty()) {
            build += " - " + revision;
        }
        return "Hazelcast Jet" + addToName + ' ' + jetBuildInfo.getVersion() +
                " (" + build + ") starting at " + node.getThisAddress();
    }

    private String serializationVersionMessage() {
        return "Configured Hazelcast Serialization version: " + node.getBuildInfo().getSerializationVersion();
    }
}
