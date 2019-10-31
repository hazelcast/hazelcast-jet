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
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionId;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;
import com.hazelcast.jet.impl.processor.TwoTransactionProcessorUtility;
import com.hazelcast.jet.impl.util.LoggingUtil;
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

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static javax.jms.Session.AUTO_ACKNOWLEDGE;

/**
 * Private API. Access via {@link SourceProcessors#streamJmsQueueP} or {@link
 * SourceProcessors#streamJmsTopicP}.
 */
public class StreamJmsP<T> extends AbstractProcessor {

    public static final int PREFERRED_LOCAL_PARALLELISM = 1;
    private static final int COMMIT_RETRY_DELAY_MS = 100;

    private final SupplierEx<? extends Connection> newConnectionFn;
    private final FunctionEx<? super Session, ? extends MessageConsumer> consumerFn;
    private final FunctionEx<? super Message, ? extends T> projectionFn;
    private final EventTimeMapper<? super T> eventTimeMapper;

    private int transactionIndex;
    private Session session;
    private MessageConsumer consumer;
    private Traverser<Object> traverser;

    private TwoTransactionProcessorUtility<JmsTransactionId, JmsTransaction> snapshotUtility;

    StreamJmsP(
            SupplierEx<? extends Connection> newConnectionFn,
            FunctionEx<? super Session, ? extends MessageConsumer> consumerFn,
            FunctionEx<? super Message, ? extends T> projectionFn,
            EventTimePolicy<? super T> eventTimePolicy
            // TODO [viliam] add user-requested processing guarantee
    ) {
        this.newConnectionFn = newConnectionFn;
        this.consumerFn = consumerFn;
        this.projectionFn = projectionFn;

        eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
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
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        checkSerializable(newConnectionFn, "newConnectionFn");
        checkSerializable(consumerFn, "consumerFn");
        checkSerializable(projectionFn, "projectionFn");

        return () -> new StreamJmsP<>(newConnectionFn, consumerFn, projectionFn, eventTimePolicy);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        Connection connection = newConnectionFn.get();
        connection.start();
        if (context.processingGuarantee() == ProcessingGuarantee.EXACTLY_ONCE) {
            if (!(connection instanceof XAConnection)) {
                throw new JetException("For exactly-once mode the connection must be a " + XAConnection.class.getName());
            }
            session = ((XAConnection) connection).createXASession();
        } else {
            session = connection.createSession(false, AUTO_ACKNOWLEDGE);
        }
        snapshotUtility = new TwoTransactionProcessorUtility<>(getOutbox(), context, context.processingGuarantee(),
                transactional -> {
                    JmsTransactionId txnId = new JmsTransactionId(context, context.globalProcessorIndex(),
                            transactionIndex++);
                    if (transactional) {
                        // rollback the transaction first before trying to use it. If it was used before and unfinished
                        // (such as before the 1st snapshot), we could fail to start it again with XAER_DUPID.
                        try {
                            LoggingUtil.logFine(getLogger(), "Preemptively rolling back %s", txnId);
                            ((XASession) session).getXAResource().rollback(txnId);
                        } catch (XAException e) {
                            if (e.errorCode != XAException.XAER_NOTA) {
                                throw handleXAException(e, txnId);
                            }
                        }
                    }
                    return new JmsTransaction((XASession) session, txnId);
                },
                txnId -> {
                    try {
                        recoverTransaction(txnId, true);
                    } catch (Exception e) {
                        context.logger().warning("Failed to finish the commit of a transaction ID saved in the " +
                                "snapshot, data loss can occur. Transaction id: " + txnId, e);
                    }
                },
                index -> {
                    recoverTransaction(new JmsTransactionId(context, index, 0), false);
                    recoverTransaction(new JmsTransactionId(context, index, 1), false);
                }
        );
        consumer = consumerFn.apply(session);
        traverser = ((Traverser<Message>) () -> uncheckCall(() ->
                        snapshotUtility.activeTransaction() != null ? consumer.receiveNoWait() : null))
                .flatMap(t -> eventTimeMapper.flatMapEvent(projectionFn.apply(t), 0, handleJmsTimestamp(t)));
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private void recoverTransaction(Xid xid, boolean commit) throws InterruptedException {
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
        emitFromTraverser(traverser);
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

    @SuppressWarnings("unchecked")
    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        // TODO [viliam] remove casting here
        // TODO [viliam] we must use the other transaction than the one we restored!!
        snapshotUtility.restoreFromSnapshot(entry((BroadcastKey<JmsTransactionId>) key, (Boolean) value));
    }

    @Override
    public boolean finishSnapshotRestore() {
        return snapshotUtility.finishSnapshotRestore();
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

    private final class JmsTransaction implements TransactionalResource<JmsTransactionId> {

        private final XASession session;
        private final JmsTransactionId txnId;

        JmsTransaction(XASession session, JmsTransactionId txnId) {
            this.session = session;
            this.txnId = txnId;
        }

        @Override
        public JmsTransactionId id() {
            return txnId;
        }

        @Override
        public void beginTransaction() throws Exception {
            LoggingUtil.logFine(getLogger(), "start, %s", txnId); // TODO [viliam] use finest
            try {
                session.getXAResource().start(txnId, XAResource.TMNOFLAGS);
            } catch (XAException e) {
                throw handleXAException(e, txnId);
            }
        }

        @Override
        public void prepare() throws Exception {
            LoggingUtil.logFine(getLogger(), "end & prepare, %s", txnId); // TODO [viliam] use finest
            try {
                session.getXAResource().end(txnId, XAResource.TMSUCCESS);
                int res = session.getXAResource().prepare(txnId);
                if (res == XAResource.XA_RDONLY) {
                    // According to X/Open Distributed Transaction Specification, if prepare
                    // returns XA_RDONLY, the transactions is non-existent afterwards. There's
                    // no need to call commit and the commit can even fail
                    // TODO [viliam] ensure that we don't call the commit
                }
            } catch (XAException e) {
                throw handleXAException(e, txnId);
            }
        }

        @Override
        public void commit() throws Exception {
            LoggingUtil.logFine(getLogger(), "commit, %s",  txnId); // TODO [viliam] use finest
            try {
                session.getXAResource().commit(txnId, false);
            } catch (XAException e) {
                throw handleXAException(e, txnId);
            }
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
