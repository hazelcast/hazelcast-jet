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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionId;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;
import com.hazelcast.jet.impl.processor.TwoTransactionProcessorUtility;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.transaction.impl.xa.SerializableXID;

import javax.annotation.Nonnull;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.nio.charset.StandardCharsets;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * Private API. Access via {@link Sources#jmsTopic} or {@link
 * Sources#jmsQueue}.
 */
public class StreamJmsP<T> extends AbstractProcessor {

    public static final int PREFERRED_LOCAL_PARALLELISM = 1;
    private static final int COMMIT_RETRY_DELAY_MS = 100;

    private final SupplierEx<? extends Connection> newConnectionFn;
    private final FunctionEx<? super Session, ? extends MessageConsumer> consumerFn;
    private final FunctionEx<? super Message, ? extends T> projectionFn;
    private final EventTimeMapper<? super T> eventTimeMapper;
    private final ProcessingGuarantee maxGuarantee;

    private Session session;
    private MessageConsumer consumer;
    private Traverser<Object> pendingTraverser = Traversers.empty();

    private TwoTransactionProcessorUtility<JmsTransactionId, TransactionalResource<JmsTransactionId>> snapshotUtility;

    StreamJmsP(
            SupplierEx<? extends Connection> newConnectionFn,
            FunctionEx<? super Session, ? extends MessageConsumer> consumerFn,
            FunctionEx<? super Message, ? extends T> projectionFn,
            EventTimePolicy<? super T> eventTimePolicy,
            ProcessingGuarantee maxGuarantee
    ) {
        this.newConnectionFn = newConnectionFn;
        this.consumerFn = consumerFn;
        this.projectionFn = projectionFn;

        eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        this.maxGuarantee = maxGuarantee;
        eventTimeMapper.addPartitions(1);
    }

    /**
     * Private API. Use {@link SourceProcessors#streamJmsQueueP} or {@link
     * SourceProcessors#streamJmsTopicP} instead.
     */
    @Nonnull
    public static <T> SupplierEx<Processor> supplier(
            @Nonnull SupplierEx<? extends Connection> newConnectionFn,
            @Nonnull FunctionEx<? super Session, ? extends MessageConsumer> consumerFn,
            @Nonnull FunctionEx<? super Message, ? extends T> projectionFn,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy,
            ProcessingGuarantee sourceGuarantee
    ) {
        checkSerializable(newConnectionFn, "newConnectionFn");
        checkSerializable(consumerFn, "consumerFn");
        checkSerializable(projectionFn, "projectionFn");

        return () -> new StreamJmsP<>(newConnectionFn, consumerFn, projectionFn, eventTimePolicy, sourceGuarantee);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        Connection connection = newConnectionFn.get();
        connection.start();

        ProcessingGuarantee guarantee = Util.min(maxGuarantee, context.processingGuarantee());
        if (guarantee == EXACTLY_ONCE) {
            if (!(connection instanceof XAConnection)) {
                throw new JetException("For exactly-once mode the connection must be a " + XAConnection.class.getName());
            }
            session = ((XAConnection) connection).createXASession();
        } else {
            session = connection.createSession(guarantee == AT_LEAST_ONCE, Session.DUPS_OK_ACKNOWLEDGE);
        }

        snapshotUtility = new TwoTransactionProcessorUtility<>(getOutbox(), context, guarantee,
                false,
                (processorIndex, txnIndex) -> new JmsTransactionId(context, processorIndex, txnIndex),
                txnId -> {
                    if (session instanceof XASession) {
                        // rollback the transaction first before trying to use it. If it was used before and is
                        // unfinished (such as before the 1st snapshot), we could fail to start it again with
                        // XAER_DUPID.
                        try {
                            LoggingUtil.logFine(getLogger(), "Preemptively rolling back %s", txnId);
                            ((XASession) session).getXAResource().rollback(txnId);
                        } catch (XAException e) {
                            if (e.errorCode != XAException.XAER_NOTA) {
                                throw handleXAException(e, txnId);
                            }
                        }
                        return new JmsXATransaction((XASession) session, txnId);
                    } else {
                        return new JmsTransaction(session, txnId);
                    }
                },
                txnId -> {
                    try {
                        recoverTransaction(txnId, true);
                    } catch (Exception e) {
                        context.logger().warning("Failed to finish the commit of a transaction ID saved in the " +
                                "snapshot, data loss can occur. Transaction id: " + txnId, e);
                    }
                },
                txnId -> recoverTransaction(txnId, false)
        );
        consumer = consumerFn.apply(session);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private void recoverTransaction(Xid xid, boolean commit) throws InterruptedException {
        if (!(session instanceof XASession)) {
            return;
        }
        XASession session = (XASession) this.session;
        if (commit) {
            for (;;) {
                try {
                    session.getXAResource().commit(xid, false);
                    getLogger().info("Successfully committed restored transaction ID: " + xid);
                } catch (XAException e) {
                    switch (e.errorCode) {
                        case XAException.XA_RETRY:
                            LoggingUtil.logFine(getLogger(), "Commit failed with XA_RETRY, will retry in %s ms. XID: %s",
                                    COMMIT_RETRY_DELAY_MS, xid);
                            Thread.sleep(COMMIT_RETRY_DELAY_MS);
                            continue;
                        case XAException.XA_HEURCOM:
                            LoggingUtil.logFine(getLogger(), "Due to a heuristic decision, the work done on behalf of " +
                                    "the specified transaction branch was already committed. Transaction ID: %s", xid);
                            break;
                        case XAException.XA_HEURRB:
                            getLogger().warning("Due to a heuristic decision, the work done on behalf of the restored " +
                                    "transaction ID was rolled back. Messages written in that transaction are lost. " +
                                    "Transaction ID: " + xid, handleXAException(e, xid));
                            break;
                        case XAException.XAER_NOTA:
                            LoggingUtil.logFine(getLogger(), "Failed to commit XID restored from snapshot: The " +
                                    "specified XID is not known to the resource manager. This happens normally when the " +
                                    "transaction was committed in phase 2 of the snapshot and can be ignored, but can " +
                                    "happen also if the transaction wasn't committed in phase 2 and the RM lost it (in " +
                                    "this case data written in it is lost). Transaction ID: %s", xid);
                            break;
                        default:
                            getLogger().warning("Failed to commit XID restored from the snapshot, XA error code: " +
                                    e.errorCode + ". Data loss is possible. Transaction ID: " + xid,
                                    handleXAException(e, xid));
                    }
                }
                break;
            }
        } else {
            try {
                session.getXAResource().rollback(xid);
            } catch (XAException e) {
                // we ignore rollback failures
                LoggingUtil.logFine(getLogger(), "Failed to rollback transaction, transaction ID: %s. Error: %s",
                        xid, handleXAException(e, xid));
            }
        }
    }

    private static long handleJmsTimestamp(Message msg) {
        try {
            // as per `getJMSTimestamp` javadoc, it can return 0 if the timestamp was optimized away
            return msg.getJMSTimestamp() == 0 ? EventTimeMapper.NO_NATIVE_TIME : msg.getJMSTimestamp();
        } catch (JMSException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public boolean complete() {
        if (snapshotUtility.isSnapshotInProgress()) {
            return false;
        }
        boolean hasMoreMsgs = snapshotUtility.activeTransaction() != null;
        while (emitFromTraverser(pendingTraverser) && hasMoreMsgs) {
            try {
                Message t = consumer.receiveNoWait();
                hasMoreMsgs = t != null;
                pendingTraverser = hasMoreMsgs
                        ? eventTimeMapper.flatMapEvent(projectionFn.apply(t), 0, handleJmsTimestamp(t))
                        : eventTimeMapper.flatMapIdle();
            } catch (JMSException e) {
                throw sneakyThrow(e);
            }
        }
        return false;
    }

    @Override
    public boolean saveToSnapshot() {
        return snapshotUtility.saveToSnapshot();
    }

    @Override
    public boolean onSnapshotCompleted(boolean commitTransactions) {
        return snapshotUtility.onSnapshotCompleted(commitTransactions);
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        snapshotUtility.restoreFromSnapshot(key, value);
    }

    @Override
    public void close() throws Exception {
        if (snapshotUtility != null) {
            snapshotUtility.close();
        }
        if (consumer != null) {
            consumer.close();
        }
        if (session != null) {
            session.close();
        }
    }

    private static final class JmsTransactionId extends SerializableXID implements TransactionId {

        /**
         * Number generated out of thin air.
         */
        private static final int JET_FORMAT_ID = 275827911;

        private static final int OFFSET_JOB_ID = 0;
        private static final int OFFSET_JOB_NAME_HASH = OFFSET_JOB_ID + Bits.LONG_SIZE_IN_BYTES;
        private static final int OFFSET_VERTEX_ID_HASH = OFFSET_JOB_NAME_HASH + Bits.LONG_SIZE_IN_BYTES;
        private static final int OFFSET_PROCESSOR_INDEX = OFFSET_VERTEX_ID_HASH + Bits.LONG_SIZE_IN_BYTES;
        private static final int OFFSET_TRANSACTION_INDEX = OFFSET_PROCESSOR_INDEX + Bits.INT_SIZE_IN_BYTES;
        private static final int GTRID_LENGTH = OFFSET_TRANSACTION_INDEX + Bits.INT_SIZE_IN_BYTES;

        @SuppressWarnings("unused") // needed for deserialization
        private JmsTransactionId() {
        }

        private JmsTransactionId(Processor.Context context, int processorIndex, int transactionIndex) {
            super(JET_FORMAT_ID,
                    createGtrid(context.jobId(),
                            stringHash(context.jobConfig().getName()),
                            stringHash(context.vertexName()),
                            processorIndex, transactionIndex),
                    new byte[1]);
        }

        private static long stringHash(String string) {
            byte[] bytes = String.valueOf(string).getBytes(StandardCharsets.UTF_8);
            return HashUtil.MurmurHash3_x64_64(bytes, 0, bytes.length);
        }

        private static byte[] createGtrid(
                long jobId,
                long jobNameHash,
                long vertexIdHash,
                int processorIndex,
                int transactionIndex
        ) {
            byte[] res = new byte[GTRID_LENGTH];
            Bits.writeLong(res, OFFSET_JOB_ID, jobId, true);
            Bits.writeLong(res, OFFSET_JOB_NAME_HASH, jobNameHash, true);
            Bits.writeLong(res, OFFSET_VERTEX_ID_HASH, vertexIdHash, true);
            Bits.writeInt(res, OFFSET_PROCESSOR_INDEX, processorIndex, true);
            Bits.writeInt(res, OFFSET_TRANSACTION_INDEX, transactionIndex, true);
            return res;
        }

        @Override
        public int index() {
            return Bits.readInt(getGlobalTransactionId(), OFFSET_PROCESSOR_INDEX, true);
        }

        @Override
        public String toString() {
            return JmsTransactionId.class.getSimpleName() + "{"
                    + "jobId=" + idToString(Bits.readLong(getGlobalTransactionId(), OFFSET_JOB_ID, true)) + ", "
                    + "jobNameHash=" + Bits.readLong(getGlobalTransactionId(), OFFSET_JOB_NAME_HASH, true) + ", "
                    + "vertexIdHash=" + Bits.readLong(getGlobalTransactionId(), OFFSET_VERTEX_ID_HASH, true) + ", "
                    + "processorIndex=" + index() + ", "
                    + "transactionIndex=" + Bits.readInt(getGlobalTransactionId(), OFFSET_TRANSACTION_INDEX, true) + "}";
        }
    }

    private final class JmsXATransaction implements TransactionalResource<JmsTransactionId> {

        private final XAResource xaResource;
        private final JmsTransactionId txnId;
        private boolean readOnlyInPrepare;

        JmsXATransaction(XASession session, JmsTransactionId txnId) {
            this.xaResource = session.getXAResource();
            this.txnId = txnId;
        }

        @Override
        public JmsTransactionId id() {
            return txnId;
        }

        @Override
        public void begin() throws Exception {
            LoggingUtil.logFine(getLogger(), "start, %s", txnId);
            try {
                xaResource.start(txnId, XAResource.TMNOFLAGS);
            } catch (XAException e) {
                throw handleXAException(e, txnId);
            }
        }

        @Override
        public boolean flush() {
            return emitFromTraverser(pendingTraverser);
        }

        @Override
        public void endAndPrepare() throws Exception {
            LoggingUtil.logFine(getLogger(), "end & prepare, %s", txnId);
            try {
                xaResource.end(txnId, XAResource.TMSUCCESS);
                int res = xaResource.prepare(txnId);
                if (res == XAResource.XA_RDONLY) {
                    // According to X/Open Distributed Transaction Specification, if prepare
                    // returns XA_RDONLY, the transactions is non-existent afterwards. There's
                    // no need to call commit and the commit can even fail
                    readOnlyInPrepare = true;
                }
            } catch (XAException e) {
                throw handleXAException(e, txnId);
            }
        }

        @Override
        public void commit() throws Exception {
            try {
                if (readOnlyInPrepare) {
                    readOnlyInPrepare = false;
                    LoggingUtil.logFine(getLogger(), "commit ignored, transaction returned XA_RDONLY in prepare, %s",
                            txnId);
                } else {
                    LoggingUtil.logFine(getLogger(), "commit, %s",  txnId);
                    xaResource.commit(txnId, false);
                }
            } catch (XAException e) {
                throw handleXAException(e, txnId);
            }
        }

        @Override
        public void release() {
        }
    }

    private final class JmsTransaction implements TransactionalResource<JmsTransactionId> {

        private final Session session;
        private final JmsTransactionId txnId;

        JmsTransaction(Session session, JmsTransactionId txnId) {
            this.session = session;
            this.txnId = txnId;
        }

        @Override
        public JmsTransactionId id() {
            return txnId;
        }

        @Override
        public void begin() {
        }

        @Override
        public boolean flush() {
            return emitFromTraverser(pendingTraverser);
        }

        @Override
        public void endAndPrepare() {
        }

        @Override
        public void commit() throws Exception {
            getLogger().fine("commit");
            session.commit();
        }

        @Override
        public void release() {
        }
    }

    private XAException handleXAException(XAException e, Xid xid) {
        if (e.getMessage() == null) {
            // workaround for https://issues.apache.org/jira/browse/ARTEMIS-2532
            // the exception has `errorCode` field, but no message, let's throw a new one with errorCode in the message
            return new BetterXAException("errorCode=" + e.errorCode + (xid != null ? ", xid=" + xid : ""), e.errorCode, e);
        }
        return e;
    }

    private static final class BetterXAException extends XAException {
        private BetterXAException(String message, int errorCode, Throwable cause) {
            super(message);
            initCause(cause);
            this.errorCode = errorCode;
        }
    }
}
