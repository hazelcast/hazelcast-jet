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
import com.hazelcast.jet.impl.processor.TransactionPoolSnapshotUtility;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionId;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;
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
import static javax.jms.Session.DUPS_OK_ACKNOWLEDGE;
import static javax.transaction.xa.XAException.XA_RETRY;

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

    private Connection connection;
    private XASession xaSession;
    private MessageConsumer xaConsumer;
    private Traverser<Object> pendingTraverser = Traversers.empty();

    private TransactionPoolSnapshotUtility<JmsTransactionId, JmsResource> snapshotUtility;

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

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        connection = newConnectionFn.get();
        connection.start();

        ProcessingGuarantee guarantee = Util.min(maxGuarantee, context.processingGuarantee());
        if (guarantee == EXACTLY_ONCE) {
            if (!(connection instanceof XAConnection)) {
                throw new JetException("For exactly-once mode the connection must be a " + XAConnection.class.getName());
            }
            xaSession = ((XAConnection) connection).createXASession();
            xaConsumer = consumerFn.apply(xaSession);
        }

        snapshotUtility = new TransactionPoolSnapshotUtility<>(getOutbox(), context, true, guarantee, 2,
                (processorIndex, txnIndex) -> new JmsTransactionId(context, processorIndex, txnIndex),
                txnId -> {
                    if (xaSession != null) {
                        // rollback the transaction first before trying to use it. If it was used before and is
                        // unfinished (such as before the 1st snapshot), we could fail to start it again with
                        // XAER_DUPID.
                        try {
                            LoggingUtil.logFine(getLogger(), "Preemptively rolling back %s", txnId);
                            xaSession.getXAResource().rollback(txnId);
                        } catch (XAException e) {
                            if (e.errorCode != XAException.XAER_NOTA) {
                                throw handleXAException(e, txnId);
                            }
                        }
                        return new JmsXATransaction(txnId);
                    } else {
                        return new JmsTransaction(txnId);
                    }
                },
                this::recoverTransaction,
                this::abortTransaction);
    }

    @Override
    public boolean complete() {
        JmsResource activeTransaction = snapshotUtility.activeTransaction();
        if (activeTransaction == null) {
            return false;
        }
        while (emitFromTraverser(pendingTraverser)) {
            try {
                Message t = activeTransaction.consumer().receiveNoWait();
                if (t == null) {
                    pendingTraverser = eventTimeMapper.flatMapIdle();
                    break;
                }
                pendingTraverser = eventTimeMapper.flatMapEvent(projectionFn.apply(t), 0, handleJmsTimestamp(t));
            } catch (JMSException e) {
                throw sneakyThrow(e);
            }
        }
        return false;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private void recoverTransaction(Xid xid) throws InterruptedException {
        if (xaSession == null) {
            return;
        }
        for (;;) {
            try {
                xaSession.getXAResource().commit(xid, false);
                getLogger().info("Successfully committed restored transaction ID: " + xid);
            } catch (XAException e) {
                switch (e.errorCode) {
                    case XA_RETRY:
                        LoggingUtil.logFine(getLogger(), "Commit failed with XA_RETRY, will retry in %s ms. XID: %s",
                                COMMIT_RETRY_DELAY_MS, xid);
                        Thread.sleep(COMMIT_RETRY_DELAY_MS);
                        LoggingUtil.logFine(getLogger(), "Retrying commit %s", xid);
                        continue;
                    case XAException.XA_HEURCOM:
                        LoggingUtil.logFine(getLogger(), "Due to a heuristic decision, the work done on behalf of " +
                                "the specified transaction branch was already committed. Transaction ID: %s", xid);
                        break;
                    case XAException.XA_HEURRB:
                        getLogger().warning("Due to a heuristic decision, the work done on behalf of the restored " +
                                "transaction ID was rolled back. Messages written in that transaction are lost. " +
                                "Ignoring the problem and will continue the job. Transaction ID: " + xid,
                                handleXAException(e, xid));
                        break;
                    case XAException.XAER_NOTA:
                        LoggingUtil.logFine(getLogger(), "Failed to commit XID restored from snapshot: The " +
                                "specified XID is not known to the resource manager. This happens normally when the " +
                                "transaction was committed in phase 2 of the snapshot and can be ignored, but can " +
                                "happen also if the transaction wasn't committed in phase 2 and the RM lost it (in " +
                                "this case data written in it is lost). Transaction ID: %s", xid);
                        break;
                    default:
                        throw new JetException("Failed to commit XID restored from the snapshot, XA error code: " +
                                e.errorCode + ". Data loss is possible. Transaction ID: " + xid + ", cause: " + e,
                                handleXAException(e, xid));
                }
            }
            break;
        }
    }

    private void abortTransaction(Xid xid) {
        if (xaSession == null) {
            return;
        }
        try {
            xaSession.getXAResource().rollback(xid);
        } catch (XAException e) {
            // we ignore rollback failures. XAER_NOTA is "transaction doesn't exist", this is the normal case,
            // we don't even log it
            if (e.errorCode != XAException.XAER_NOTA) {
                LoggingUtil.logFine(getLogger(), "Failed to rollback, transaction ID: %s. Error: %s",
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
    public boolean snapshotPrepareCommit() {
        return snapshotUtility.snapshotPrepareCommit();
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
        if (xaSession != null) {
            xaSession.close();
        }
        if (connection != null) {
            getLogger().fine("closing connection");
            connection.close();
        }
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

    private abstract class JmsResource implements TransactionalResource<JmsTransactionId> {
        final JmsTransactionId txnId;

        JmsResource(JmsTransactionId txnId) {
            this.txnId = txnId;
        }

        @Nonnull
        abstract MessageConsumer consumer();

        @Override
        public JmsTransactionId id() {
            return txnId;
        }

        @Override
        public boolean flush() {
            return emitFromTraverser(pendingTraverser);
        }
    }

    private final class JmsXATransaction extends JmsResource {

        private final XAResource xaResource;

        private boolean readOnlyInPrepare;

        JmsXATransaction(JmsTransactionId txnId) {
            super(txnId);
            this.xaResource = xaSession.getXAResource();
        }

        @Nonnull @Override
        public MessageConsumer consumer() {
            return xaConsumer;
        }

        @Override
        public void begin() throws Exception {
            try {
                xaResource.start(txnId, XAResource.TMNOFLAGS);
            } catch (XAException e) {
                throw handleXAException(e, txnId);
            }
        }

        @Override
        public void endAndPrepare() throws Exception {
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
            if (readOnlyInPrepare) {
                readOnlyInPrepare = false;
                LoggingUtil.logFine(getLogger(), "commit ignored, transaction returned XA_RDONLY in prepare, %s",
                        txnId);
            } else {
                for (;;) {
                    try {
                        xaResource.commit(txnId, false);
                        break;
                    } catch (XAException e) {
                        if (e.errorCode == XA_RETRY) {
                            continue;
                        }
                        throw handleXAException(e, txnId);
                    }
                }
            }
        }

        @Override
        public void rollback() throws Exception {
            try {
                xaResource.end(txnId, XAResource.TMFAIL);
                xaResource.rollback(txnId);
            } catch (XAException e) {
                throw handleXAException(e, txnId);
            }
        }

        @Override
        public void release() {
        }
    }

    private final class JmsTransaction extends JmsResource {

        private final Session session;
        private final MessageConsumer consumer;

        JmsTransaction(JmsTransactionId txnId) throws JMSException {
            super(txnId);
            boolean transacted = snapshotUtility.externalGuarantee() == AT_LEAST_ONCE;
            this.session = connection.createSession(transacted, DUPS_OK_ACKNOWLEDGE);
            this.consumer = consumerFn.apply(session);
        }

        @Nonnull @Override
        public MessageConsumer consumer() {
            return consumer;
        }

        @Override
        public void begin() {
        }

        @Override
        public void commit() throws Exception {
            session.commit();
        }

        @Override
        public void rollback() throws Exception {
            session.rollback();
        }

        @Override
        public void release() throws JMSException {
            session.close();
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
