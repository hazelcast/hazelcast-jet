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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.SenderTasklet;
import com.hazelcast.jet.impl.serialization.DataInput;
import com.hazelcast.jet.impl.serialization.DataInputFactory;
import com.hazelcast.jet.impl.serialization.DataOutput;
import com.hazelcast.jet.impl.serialization.DataOutputFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;

import static com.hazelcast.internal.nio.Packet.FLAG_JET_FLOW_CONTROL;
import static com.hazelcast.internal.nio.Packet.FLAG_URGENT;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.ImdgUtil.getMemberConnection;
import static com.hazelcast.jet.impl.util.ImdgUtil.getRemoteMembers;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Networking {

    private static final int PACKET_HEADER_SIZE = 16;
    private static final int FLOW_PACKET_INITIAL_SIZE = 128; // TODO:

    private static final byte[] EMPTY_BYTES = new byte[0];

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final JobExecutionService jobExecutionService;
    private final ScheduledFuture<?> flowControlSender;

    Networking(NodeEngine nodeEngine, JobExecutionService jobExecutionService, int flowControlPeriodMs) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.jobExecutionService = jobExecutionService;
        this.flowControlSender = nodeEngine.getExecutionService().scheduleWithRepetition(
                this::broadcastFlowControlPacket, 0, flowControlPeriodMs, MILLISECONDS);
    }

    void shutdown() {
        flowControlSender.cancel(false);
    }

    void handle(Packet packet) {
        if (!packet.isFlagRaised(FLAG_JET_FLOW_CONTROL)) {
            handleStreamPacket(packet);
            return;
        }
        handleFlowControlPacket(packet.getConn().getEndPoint(), packet.toByteArray());
    }

    private void handleStreamPacket(Packet packet) {
        DataInput input = DataInputFactory.from(packet.toByteArray());
        long executionId = input.readLong();
        int vertexId = input.readInt();
        int ordinal = input.readInt();
        ExecutionContext executionContext = jobExecutionService.getExecutionContext(executionId);
        executionContext.handlePacket(vertexId, ordinal, packet.getConn().getEndPoint(), input);
    }

    public static byte[] createStreamPacketHeader(long executionId, int destinationVertexId, int ordinal) {
        DataOutput output = DataOutputFactory.create(PACKET_HEADER_SIZE);
        output.writeLong(executionId);
        output.writeInt(destinationVertexId);
        output.writeInt(ordinal);
        return output.toByteArray();
    }

    private void broadcastFlowControlPacket() {
        try {
            getRemoteMembers(nodeEngine).forEach(member -> uncheckRun(() -> {
                final byte[] packetBuf = createFlowControlPacket(member);
                if (packetBuf.length == 0) {
                    return;
                }
                Connection conn = getMemberConnection(nodeEngine, member);
                if (conn != null) {
                    conn.write(new Packet(packetBuf)
                            .setPacketType(Packet.Type.JET)
                            .raiseFlags(FLAG_URGENT | FLAG_JET_FLOW_CONTROL));
                }
            }));
        } catch (Throwable t) {
            logger.severe("Flow-control packet broadcast failed", t);
        }
    }

    private byte[] createFlowControlPacket(Address member) {
        DataOutput output = DataOutputFactory.create(FLOW_PACKET_INITIAL_SIZE);
        final boolean[] hasData = {false};
        Map<Long, ExecutionContext> executionContexts = jobExecutionService.getExecutionContextsFor(member);
        output.writeInt(executionContexts.size());
        executionContexts.forEach((execId, exeCtx) -> uncheckRun(() -> {
            output.writeLong(execId);
            output.writeInt(exeCtx.receiverMap().values().stream().mapToInt(Map::size).sum());
            exeCtx.receiverMap().forEach((vertexId, ordinalToSenderToTasklet) ->
                    ordinalToSenderToTasklet.forEach((ordinal, senderToTasklet) -> uncheckRun(() -> {
                        output.writeInt(vertexId);
                        output.writeInt(ordinal);
                        output.writeInt(senderToTasklet.get(member).updateAndGetSendSeqLimitCompressed());
                        hasData[0] = true;
                    })));
        }));
        return hasData[0] ? output.toByteArray() : EMPTY_BYTES;
    }

    private void handleFlowControlPacket(Address fromAddr, byte[] packet) {
        DataInput input = DataInputFactory.from(packet);
        final int executionCtxCount = input.readInt();
        for (int j = 0; j < executionCtxCount; j++) {
            final long executionId = input.readLong();
            final Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap
                    = jobExecutionService.getSenderMap(executionId);

            if (senderMap == null) {
                logMissingExeCtx(executionId);
                continue;
            }
            final int flowCtlMsgCount = input.readInt();
            for (int k = 0; k < flowCtlMsgCount; k++) {
                int destVertexId = input.readInt();
                int destOrdinal = input.readInt();
                int sendSeqLimitCompressed = input.readInt();
                final SenderTasklet t = Optional.ofNullable(senderMap.get(destVertexId))
                                                .map(ordinalMap -> ordinalMap.get(destOrdinal))
                                                .map(addrMap -> addrMap.get(fromAddr))
                                                .orElse(null);
                if (t == null) {
                    logMissingSenderTasklet(destVertexId, destOrdinal);
                    return;
                }
                t.setSendSeqLimitCompressed(sendSeqLimitCompressed);
            }
        }
    }

    private void logMissingExeCtx(long executionId) {
        if (logger.isFinestEnabled()) {
            logger.finest("Ignoring flow control message applying to non-existent execution context "
                    + idToString(executionId));
        }
    }

    private void logMissingSenderTasklet(int destVertexId, int destOrdinal) {
        if (logger.isFinestEnabled()) {
            logger.finest(String.format(
                    "Ignoring flow control message applying to non-existent sender tasklet (%d, %d)",
                    destVertexId, destOrdinal));
        }
    }
}
