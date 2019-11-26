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

import com.hazelcast.jet.datamodel.Tuple2;
import org.mockito.stubbing.Answer;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static javax.transaction.xa.XAResource.TMFAIL;
import static javax.transaction.xa.XAResource.TMNOFLAGS;
import static javax.transaction.xa.XAResource.TMSUCCESS;
import static javax.transaction.xa.XAResource.XA_OK;
import static javax.transaction.xa.XAResource.XA_RDONLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A mock JMS broker implementing just the functionality needed for XA stress
 * tests. It implements auto-acknowledged producer&consumer, for XA
 * transactions it implements only the consumer. Doesn't implement topic, only
 * queue.
 */
final class TestJmsBroker {

    /** All non-implemented methods will throw {@code UnsupportedOperationException}. */
    private static final Answer<Object> UOE_ANSWER = invocation -> {
        throw new UnsupportedOperationException("method: " + invocation.getMethod());
    };

    private TestJmsBroker() { }

    interface CloseableXAConnectionFactory extends XAConnectionFactory, ConnectionFactory, AutoCloseable { }

    static CloseableXAConnectionFactory newTestJmsBroker() {
        BrokerState brokerState = new BrokerState();
        CloseableXAConnectionFactory cf = mock(CloseableXAConnectionFactory.class, UOE_ANSWER);

        try {
            doAnswer(invocation -> mockXAConnection(brokerState)).when(cf).createXAConnection(any(), any());
            doAnswer(invocation -> mockConnection(brokerState)).when(cf).createConnection(any(), any());
            doAnswer(invocation -> mockConnection(brokerState)).when(cf).createConnection();
            doAnswer(invocation -> {
                // make the object unusable
                brokerState.queues = null;
                brokerState.preparedTxns = null;
                return null;
            }).when(cf).close();
        } catch (Exception e) {
            throw sneakyThrow(e);
        }

        return cf;
    }

    private static XAConnection mockXAConnection(BrokerState brokerState) throws JMSException {
        XAConnection conn = mock(XAConnection.class, UOE_ANSWER);
        doAnswer(invocation -> mockXASession(brokerState)).when(conn).createXASession();
        doNothing().when(conn).start();
        doNothing().when(conn).close();
        return conn;
    }

    private static Connection mockConnection(BrokerState brokerState) throws JMSException {
        Connection conn = mock(Connection.class, UOE_ANSWER);
        doAnswer(invocation -> mockSession(brokerState)).when(conn).createSession(false, AUTO_ACKNOWLEDGE);
        doNothing().when(conn).start();
        doNothing().when(conn).close();
        return conn;
    }

    private static Session mockSession(BrokerState brokerState) throws JMSException {
        Session sess = mock(Session.class, UOE_ANSWER);

        // createProducer
        doAnswer(invocation -> {
            TestQueue queue = invocation.getArgument(0);
            return mockProducer(
                    brokerState.queues.computeIfAbsent(queue.getQueueName(), k -> new ConcurrentLinkedQueue<>()));
        }).when(sess).createProducer(any());

        doAnswer(invocation -> {
            TestQueue queue = invocation.getArgument(0);
            return mockConsumer(brokerState.queues.get(queue.getQueueName()));
        }).when(sess).createConsumer(any());

        // createTextMessage
        doAnswer(invocation -> {
            TextMessage msg = mock(TextMessage.class);
            when(msg.getText()).thenReturn(invocation.getArgument(0));
            when(msg.toString()).thenReturn(invocation.getArgument(0));
            return msg;
        }).when(sess).createTextMessage(anyString());

        // createQueue
        doAnswer(invocation -> new TestQueue(invocation.getArgument(0))).when(sess).createQueue(anyString());

        // close
        doNothing().when(sess).close();

        return sess;
    }

    private static MessageConsumer mockConsumer(ConcurrentLinkedQueue<Message> queue) throws Exception {
        MessageConsumer consumer = mock(MessageConsumer.class);
        doAnswer(invocation -> queue.poll())
                .when(consumer).receiveNoWait();
        doAnswer(invocation -> {
            Message res;
            while ((res = queue.poll()) == null) {
                LockSupport.parkNanos(MILLISECONDS.toNanos(100));
            }
            return res;
        }).when(consumer).receive();
        doNothing().when(consumer).close();
        return consumer;
    }

    private static XASession mockXASession(BrokerState brokerState) throws Exception {
        XASession sess = mock(XASession.class, UOE_ANSWER);
        XAResource xaRes = mock(XAResource.class, UOE_ANSWER);
        SessionState sessionState = new SessionState();

        // getXAResource
        doReturn(xaRes).when(sess).getXAResource();

        // createQueue
        doAnswer(invocation -> new TestQueue(invocation.getArgument(0))).when(sess).createQueue(anyString());

        // createConsumer
        doAnswer(invocation -> {
            TestQueue queue = invocation.getArgument(0);
            return mockXAConsumer(sessionState, queue.getQueueName());
        }).when(sess).createConsumer(any());

        // start
        doAnswer(invocation -> {
            assertNull("session has active txn", sessionState.activeTxn);
            Xid xid = invocation.getArgument(0);
            int flags = invocation.getArgument(1);
            assertEquals(TMNOFLAGS, flags);
            assertFalse("transaction is prepared", brokerState.preparedTxns.containsKey(xid));
            Transaction txn = sessionState.idleTxns.remove(xid);
            if (txn == null) {
                txn = new Transaction(brokerState.queues);
            }
            sessionState.activeTxn = tuple2(xid, txn);
            return null;
        }).when(xaRes).start(any(), anyInt());

        // end
        doAnswer(invocation -> {
            assertNotNull("no active txn", sessionState.activeTxn);
            Xid xid = invocation.getArgument(0);
            int flags = invocation.getArgument(1);
            assertEquals(sessionState.activeTxn.f0(), xid);
            assertTrue("flags must be TMSUCCESS or TMFAIL", flags == TMSUCCESS || flags == TMFAIL);
            if (flags == TMFAIL) {
                sessionState.activeTxn.f1().rollbackOnly = true;
            }
            assertNull(sessionState.idleTxns.put(sessionState.activeTxn.f0(), sessionState.activeTxn.f1()));
            sessionState.activeTxn = null;
            return null;
        }).when(xaRes).end(any(), anyInt());

        // prepare
        doAnswer(invocation -> {
            Xid xid = invocation.getArgument(0);
            Transaction txn = sessionState.idleTxns.remove(xid);
            assertNotNull("not an idle txn", txn);
            if (txn.unackedMessages.values().stream().mapToInt(List::size).sum() == 0) {
                return XA_RDONLY;
            } else {
                brokerState.preparedTxns.put(xid, txn);
                return XA_OK;
            }
        }).when(xaRes).prepare(any());

        // commit
        doAnswer(invocation -> {
            if (ThreadLocalRandom.current().nextInt(5) == 0) {
                throw new XAException(XAException.XA_RETRY);
            }
            Xid xid = invocation.getArgument(0);
            assertFalse(invocation.getArgument(1));
            Transaction txn = brokerState.preparedTxns.remove(xid);
            if (txn == null) {
                throw new XAException(XAException.XAER_NOTA);
            }
            txn.commit();
            return null;
        }).when(xaRes).commit(any(), anyBoolean());

        // rollback
        doAnswer(invocation -> {
            Xid xid = invocation.getArgument(0);
            Transaction txn;
            if (sessionState.activeTxn != null && xid.equals(sessionState.activeTxn.f0())) {
                sessionState.activeTxn.f1().rollback();
                sessionState.activeTxn = null;
            } else if ((txn = sessionState.idleTxns.remove(xid)) != null
                    || (txn = brokerState.preparedTxns.remove(xid)) != null) {
                txn.rollback();
            } else {
                throw new XAException(XAException.XAER_NOTA);
            }
            return null;
        }).when(xaRes).rollback(any());

        // close
        doAnswer(invocation -> {
            for (Transaction txn : sessionState.idleTxns.values()) {
                txn.rollback();
            }
            sessionState.idleTxns.clear();
            return null;
        }).when(sess).close();

        return sess;
    }

    private static MessageConsumer mockXAConsumer(SessionState sessionState, String queueName) throws JMSException {
        MessageConsumer consumer = mock(MessageConsumer.class, UOE_ANSWER);
        doAnswer(invocation -> {
            assertNotNull("no txn started", sessionState.activeTxn);
            return sessionState.activeTxn.f1().receiveDoNotAck(queueName);
        })
                .when(consumer).receiveNoWait();
        doNothing().when(consumer).close();

        return consumer;
    }

    private static MessageProducer mockProducer(ConcurrentLinkedQueue<Message> queue) throws JMSException {
        MessageProducer producer = mock(MessageProducer.class, UOE_ANSWER);
        doAnswer(invocation -> queue.add(invocation.getArgument(0))).when(producer).send(any());
        doNothing().when(producer).close();
        return producer;
    }

    private static class SessionState {
        Tuple2<Xid, Transaction> activeTxn;
        Map<Xid, Transaction> idleTxns = new HashMap<>();
    }

    private static class BrokerState {
        Map<String, ConcurrentLinkedQueue<Message>> queues = new ConcurrentHashMap<>();
        Map<Xid, Transaction> preparedTxns = new ConcurrentHashMap<>();
    }

    private static class TestQueue implements Queue {
        private final String name;

        TestQueue(String name) {
            this.name = name;
        }

        @Override
        public String getQueueName() {
            return name;
        }
    }

    private static final class Transaction {
        final Map<String, ConcurrentLinkedQueue<Message>> brokerQueues;
        final Map<String, List<Message>> unackedMessages = new HashMap<>();
        boolean rollbackOnly;

        private Transaction(Map<String, ConcurrentLinkedQueue<Message>> brokerQueues) {
            this.brokerQueues = brokerQueues;
        }

        private Message receiveDoNotAck(String queueName) {
            ConcurrentLinkedQueue<Message> queue = brokerQueues.get(queueName);
            if (queue != null) {
                Message res = queue.poll();
                if (res != null) {
                    unackedMessages.computeIfAbsent(queueName, k -> new LinkedList<>())
                                   .add(res);
                }
                return res;
            }
            return null;
        }

        private void commit() {
            assertFalse("txn is rollback-only", rollbackOnly);
            unackedMessages.clear();
        }

        private void rollback() {
            for (Entry<String, List<Message>> entry : unackedMessages.entrySet()) {
                // put unacked messages back to broker's queues
                brokerQueues.get(entry.getKey())
                            .addAll(entry.getValue());
            }
            unackedMessages.clear();
        }
    }
}
