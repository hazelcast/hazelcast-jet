package com.hazelcast.jet.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.instance.JetBuildInfo;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;

import static com.hazelcast.cluster.ClusterState.PASSIVE;

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
        if (requestedState == PASSIVE) {
            jetService.shutDownJobs();
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
