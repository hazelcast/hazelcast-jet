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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.impl.util.ObjectWithPartitionId;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.function.Predicate;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.jet.impl.Networking.createStreamPacketHeader;
import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.execution.ReceiverTasklet.compressSeq;
import static com.hazelcast.jet.impl.execution.ReceiverTasklet.estimatedMemoryFootprint;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.Util.createObjectDataOutput;
import static com.hazelcast.jet.impl.util.Util.getMemberConnection;
import static com.hazelcast.jet.impl.util.Util.lazyAdd;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

public class SenderTasklet implements Tasklet {

    private final Connection connection;
    private final Queue<Object> inbox = new ArrayDeque<>();
    private final ProgressTracker progTracker = new ProgressTracker();
    private final InboundEdgeStream inboundEdgeStream;
    private final BufferObjectDataOutput outputBuffer;
    private final int bufPosPastHeader;
    private final int packetSizeLimit;
    private final AtomicLong itemsOutCounter = new AtomicLong();
    private final AtomicLong bytesOutCounter = new AtomicLong();

    private boolean instreamExhausted;
    // read and written by Jet thread
    private long sentSeq;

    // Written by HZ networking thread, read by Jet thread
    private volatile int sendSeqLimitCompressed;
    private Predicate<Object> addToInboxFunction = inbox::add;

    public SenderTasklet(InboundEdgeStream inboundEdgeStream, NodeEngine nodeEngine, Address destinationAddress,
                         long executionId, int destinationVertexId, int packetSizeLimit) {
        this.inboundEdgeStream = inboundEdgeStream;
        this.packetSizeLimit = packetSizeLimit;
        // we use Connection directly because we rely on packets not being transparently skipped or reordered
        this.connection = getMemberConnection(nodeEngine, destinationAddress);
        this.outputBuffer = createObjectDataOutput(nodeEngine);
        uncheckRun(() -> outputBuffer.write(createStreamPacketHeader(
                nodeEngine, executionId, destinationVertexId, inboundEdgeStream.ordinal())));
        bufPosPastHeader = outputBuffer.position();
    }

    @Nonnull @Override
    public ProgressState call() {
        progTracker.reset();
        tryFillInbox();
        if (progTracker.isDone()) {
            return progTracker.toProgressState();
        }
        if (tryFillOutputBuffer()) {
            progTracker.madeProgress();
            if (!connection.write(new Packet(outputBuffer.toByteArray()).setPacketType(Packet.Type.JET))) {
                throw new RestartableException("Connection write failed in " + toString());
            }
        }
        return progTracker.toProgressState();
    }

    private void tryFillInbox() {
        if (!inbox.isEmpty()) {
            progTracker.notDone();
            return;
        }
        if (instreamExhausted) {
            return;
        }
        progTracker.notDone();
        final ProgressState result = inboundEdgeStream.drainTo(addToInboxFunction);
        progTracker.madeProgress(result.isMadeProgress());
        instreamExhausted = result.isDone();
        if (instreamExhausted) {
            inbox.add(new ObjectWithPartitionId(DONE_ITEM, -1));
        }
    }

    private boolean tryFillOutputBuffer() {
        try {
            // header size + slot for writtenCount
            outputBuffer.position(bufPosPastHeader + Bits.INT_SIZE_IN_BYTES);
            int writtenCount = 0;
            for (Object item;
                 outputBuffer.position() < packetSizeLimit
                         && isWithinLimit(sentSeq, sendSeqLimitCompressed)
                         && (item = inbox.poll()) != null;
                 writtenCount++
            ) {
                ObjectWithPartitionId itemWithPId = item instanceof ObjectWithPartitionId ?
                        (ObjectWithPartitionId) item : new ObjectWithPartitionId(item, - 1);
                final int mark = outputBuffer.position();
                outputBuffer.writeObject(itemWithPId.getItem());
                sentSeq += estimatedMemoryFootprint(outputBuffer.position() - mark);
                outputBuffer.writeInt(itemWithPId.getPartitionId());
            }
            outputBuffer.writeInt(bufPosPastHeader, writtenCount);
            lazyAdd(bytesOutCounter, outputBuffer.position());
            lazyAdd(itemsOutCounter, writtenCount);
            return writtenCount > 0;
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    /**
     * Updates the upper limit on {@link #sentSeq}, which constrains how much more data this tasklet can send.
     *
     * @param sendSeqLimitCompressed the compressed seq read from a flow-control message. The method
     *                               {@link #isWithinLimit(long, int)} derives the limit on the uncompressed
     *                               {@code sentSeq} from the number supplied here.
     */
    // Called from HZ networking thread
    public void setSendSeqLimitCompressed(int sendSeqLimitCompressed) {
        this.sendSeqLimitCompressed = sendSeqLimitCompressed;
    }

    @Override
    public String toString() {
        return "SenderTasklet " + connection.getEndPoint();
    }

    /**
     * Given an uncompressed {@code sentSeq} and a compressed {@code sendSeqLimitCompressed}, tells
     * whether the {@code sentSeq} is within the limit specified by the compressed seq.
     */
    // The operations and types in this method must be carefully chosen to properly
    // handle wrap-around that is allowed to happen on sendSeqLimitCompressed.
    static boolean isWithinLimit(long sentSeq, int sendSeqLimitCompressed) {
        return compressSeq(sentSeq) - sendSeqLimitCompressed <= 0;
    }

    public AtomicLong getItemsOutCounter() {
        return itemsOutCounter;
    }

    public AtomicLong getBytesOutCounter() {
        return bytesOutCounter;
    }
}
