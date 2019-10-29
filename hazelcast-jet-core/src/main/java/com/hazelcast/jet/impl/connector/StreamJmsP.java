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
import java.util.Arrays;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static javax.jms.Session.AUTO_ACKNOWLEDGE;

/**
 * Private API. Access via {@link SourceProcessors#streamJmsQueueP} or {@link
 * SourceProcessors#streamJmsTopicP}.
 * <p>
 * Since we use a non-blocking version of JMS consumer API, the processor is
 * marked as cooperative.
 */
public class StreamJmsP<T> extends AbstractProcessor {

    public static final int PREFERRED_LOCAL_PARALLELISM = 4;

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
        if (snapshotUtility.externalGuarantee() == ProcessingGuarantee.EXACTLY_ONCE) {
            if (!(connection instanceof XAConnection)) {
                throw new JetException("For exactly-once mode the connection must be a " + XAConnection.class.getName());
            }
            session = ((XAConnection) connection).createXASession();
        } else {
            session = connection.createSession(false, AUTO_ACKNOWLEDGE);
        }
        consumer = consumerFn.apply(session);
        traverser = ((Traverser<Message>) () -> uncheckCall(() -> consumer.receiveNoWait()))
                .flatMap(t -> eventTimeMapper.flatMapEvent(projectionFn.apply(t), 0, handleJmsTimestamp(t)));

        snapshotUtility = new TwoTransactionProcessorUtility<>(getOutbox(), context, context.processingGuarantee(),
                transactional -> new JmsTransaction((XASession) session,
                        new JmsTransactionId(context, context.globalProcessorIndex(), transactionIndex++)),
                txnId -> {
                    try {
                        recoverTransaction(txnId.xid, true);
                    } catch (Exception e) {
                        context.logger().warning("Failed to finish the commit of a transaction ID saved in the " +
                                "snapshot, data loss can occur. Transaction id: " + txnId, e);
                    }
                },
                index -> {
                    recoverTransaction(new JmsTransactionId(context, index, 0).xid, false);
                    recoverTransaction(new JmsTransactionId(context, index, 1).xid, false);
                }
        );
    }

    private void recoverTransaction(Xid xid, boolean commit) {
        XASession session = (XASession) this.session;
        if (commit) {
            try {
                session.getXAResource().commit(xid, false);
            } catch (XAException e) {
                switch (e.errorCode) {
                    case XAException.XA_HEURCOM:
                        getLogger().fine("Due to a heuristic decision, the work done on behalf of the specified " +
                                "transaction branch was committed. Transaction ID: " + xid, e);
                        break;
                    case XAException.XA_HEURRB:
                        getLogger().warning("Due to a heuristic decision, the work done on behalf of the specified " +
                                "transaction branch was rolled back. Messages written in that transaction are lost. " +
                                "Transaction ID: " + xid, e);
                        break;
                    case XAException.XAER_NOTA:
                        getLogger().warning("Failed to commit XID restored from snapshot: The specified XID is not " +
                                "known to the resource manager. Transaction might already be committed or was forgotten " +
                                "by the RM, data loss is possible. Transaction ID: " + xid, e);
                        break;
                    default:
                        getLogger().warning("Failed to commit XID restored from the snapshot, XA error code: " +
                                e.errorCode + ". Data loss is possible. Transaction ID: " + xid, e);
                }
            }
        } else {
            try {
                session.getXAResource().rollback(xid);
            } catch (XAException e) {
                // we ignore rollback failures
                getLogger().fine("Failed to rollback transaction, transaction ID: " + xid, e);
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
        snapshotUtility.restoreFromSnapshot(entry((BroadcastKey<JmsTransactionId>) key, (Boolean) value));
    }

    @Override
    public boolean finishSnapshotRestore() {
        return snapshotUtility.finishSnapshotRestore();
    }

    @Override
    public void close() throws Exception {
        snapshotUtility.close();
        if (consumer != null) {
            consumer.close();
        }
        if (session != null) {
            session.close();
        }
    }

    private static final class JmsTransactionId implements TransactionId {

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

        private final int transactionIndex;
        private final SerializableXID xid;

        private JmsTransactionId(Processor.Context context, int processorIndex, int transactionIndex) {
            long jobNameHash = stringHash(context.jobConfig().getName());
            long vertexIdHash = stringHash(context.vertexName());
            this.transactionIndex = transactionIndex;

            xid = new SerializableXID(JET_FORMAT_ID,
                    createGtrid(context.jobId(), jobNameHash, vertexIdHash, processorIndex, transactionIndex),
                    new byte[1]);
        }

        private JmsTransactionId(Xid xid) {
            this.xid = new SerializableXID(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());
            transactionIndex = Bits.readInt(xid.getGlobalTransactionId(), OFFSET_TRANSACTION_INDEX, true);
        }

        private static JmsTransactionId fromXid(Xid xid) {
            byte[] bytes = xid.getGlobalTransactionId();
            if (bytes == null || bytes.length != GTRID_LENGTH
                    || xid.getFormatId() != JET_FORMAT_ID
                    || !Arrays.equals(xid.getBranchQualifier(), new byte[1])) {
                return null;
            }
            return new JmsTransactionId(xid);
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
            return transactionIndex;
        }

        @Override
        public String toString() {
            return xid.toString();
        }
    }

    private static final class JmsTransaction implements TransactionalResource<JmsTransactionId> {

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
            session.getXAResource().start(txnId.xid, XAResource.TMNOFLAGS);
        }

        @Override
        public void flush(boolean finalFlush) throws Exception {
            session.getXAResource().prepare(txnId.xid);
        }

        @Override
        public void commit() throws Exception {
            session.getXAResource().commit(txnId.xid, false);
        }

        @Override
        public void release() throws Exception {
            session.getXAResource().end(txnId.xid, XAResource.TMSUSPEND);
        }
    }
}
