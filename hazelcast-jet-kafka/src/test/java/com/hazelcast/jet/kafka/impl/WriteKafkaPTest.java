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

package com.hazelcast.jet.kafka.impl;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.impl.JobProxy;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class WriteKafkaPTest extends SimpleTestInClusterSupport {

    private static final int PARTITION_COUNT = 20;

    private static KafkaTestSupport kafkaTestSupport;

    private String sourceImapName = randomMapName();
    private Properties properties;
    private String topic;
    private IMap<String, String> sourceIMap;

    @BeforeClass
    public static void beforeClass() throws IOException {
        kafkaTestSupport = new KafkaTestSupport();
        kafkaTestSupport.createKafkaCluster();
        initialize(2, null);
    }

    @Before
    public void before() {
        properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaTestSupport.getBrokerConnectionString());
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        topic = randomName();
        kafkaTestSupport.createTopic(topic, PARTITION_COUNT);

        sourceIMap = instance().getMap(sourceImapName);
        for (int i = 0; i < 20; i++) {
            sourceIMap.put(String.valueOf(i), String.valueOf(i));
        }
    }

    @AfterClass
    public static void afterClass() {
        kafkaTestSupport.shutdownKafkaCluster();
        kafkaTestSupport = null;
    }

    @Test
    public void testWriteToTopic() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.map(sourceImapName))
         .drainTo(KafkaSinks.kafka(properties, topic));
        instance().newJob(p).join();

        assertTopicContentsEventually(sourceIMap, false);
    }

    @Test
    public void testWriteToSpecificPartitions() {
        String localTopic = topic;

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<String, String>map(sourceImapName))
         .drainTo(KafkaSinks.kafka(properties, e ->
                 new ProducerRecord<>(localTopic, Integer.valueOf(e.getKey()), e.getKey(), e.getValue()))
         );
        instance().newJob(p).join();

        assertTopicContentsEventually(sourceIMap, true);
    }

    @Test
    public void when_recordLingerEnabled_then_sentOnCompletion() {
        // When
        properties.setProperty("linger.ms", "3600000"); // 1 hour

        // Given
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Entry<String, String>>batchFromProcessor("source",
                ProcessorMetaSupplier.of(ProcessorWithEntryAndLatch::new)))
         .drainTo(KafkaSinks.kafka(properties, topic));

        Job job = instance().newJob(p);

        // the event should not appear in the topic due to linger.ms
        try (KafkaConsumer<String, String> consumer = kafkaTestSupport.createConsumer(topic)) {
            assertTrueAllTheTime(() -> assertEquals(0, consumer.poll(Duration.ofMillis(100)).count()), 2);
        }

        // Then
        ProcessorWithEntryAndLatch.isDone = true;
        job.join();
        System.out.println("Job finished");
        assertTopicContentsEventually(singletonMap("k", "v"), false);
    }

    @Test
    public void when_processingGuaranteeOn_then_lingeringRecordsSentOnSnapshot_exactlyOnce() {
        when_processingGuaranteeOn_then_lingeringRecordsSentOnSnapshot(true);
    }

    @Test
    public void when_processingGuaranteeOn_then_lingeringRecordsSentOnSnapshot_atLeastOnce() {
        when_processingGuaranteeOn_then_lingeringRecordsSentOnSnapshot(false);
    }

    private void when_processingGuaranteeOn_then_lingeringRecordsSentOnSnapshot(boolean exactlyOnce) {
        // When
        properties.setProperty("linger.ms", "" + HOURS.toMillis(1));

        // Given
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Entry<String, String>>batchFromProcessor("source",
                ProcessorMetaSupplier.of(ProcessorWithEntryAndLatch::new)))
         .drainTo(KafkaSinks.<Entry<String, String>>kafka(properties)
                            .topic(topic)
                            .extractKeyFn(Entry::getKey)
                            .extractValueFn(Entry::getValue)
                            .exactlyOnce(exactlyOnce)
                            .build());

        Job job = instance().newJob(p, new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(4000));

        // the event should not appear in the topic due to linger.ms
        try (KafkaConsumer<String, String> consumer = kafkaTestSupport.createConsumer(topic)) {
            assertTrueAllTheTime(() -> assertEquals(0, consumer.poll(Duration.ofMillis(100)).count()), 2);
        }

        // Then
        ProcessorWithEntryAndLatch.allowSnapshot = true;
        assertTopicContentsEventually(singletonMap("k", "v"), false);

        ProcessorWithEntryAndLatch.isDone = true;
        job.join();
    }

    @Test
    public void test_transactional_withRestarts_graceful() {
        test_transactional_withRestarts(true);
    }

    @Test
    public void test_transactional_withRestarts_forceful() {
        test_transactional_withRestarts(false);
    }

    private void test_transactional_withRestarts(boolean graceful) {
        int numItems = 1000;
        Pipeline p = Pipeline.create();
        String topicLocal = topic;
        p.drawFrom(SourceBuilder.stream("src", procCtx -> new int[1])
                                .fillBufferFn((ctx, buf) -> {
                                    if (ctx[0] < numItems) {
                                        buf.add(ctx[0]++);
                                        sleepMillis(10);
                                    }
                                })
                                .createSnapshotFn(ctx -> ctx[0])
                                .restoreSnapshotFn((ctx, state) -> ctx[0] = state.get(0))
                                .build())
         .withoutTimestamps()
         .map(String::valueOf)
         .setLocalParallelism(1)
         // produce to a single partition to have the items sorted
         .drainTo(KafkaSinks.kafka(properties).toRecordFn(v -> new ProducerRecord<>(topicLocal, 0, null, v)).build())
         .setLocalParallelism(1);

        JobConfig config = new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(50);
        JobProxy job = (JobProxy) instance().newJob(p, config);

        try {
            KafkaConsumer<String, String> consumer = kafkaTestSupport.createConsumer(topic);
            long start = System.nanoTime();
            StringBuilder actualSinkContents = new StringBuilder();

            for (;;) {
                assertJobStatusEventually(job, RUNNING);
                sleepMillis(100);
                job.restart(graceful);
                try {
                    int actualCount = 0;
                    for (ConsumerRecords<String, String> records;
                         !(records = consumer.poll(Duration.ofSeconds(2))).isEmpty();
                    ) {
                        for (ConsumerRecord<String, String> record : records) {
                            actualSinkContents.append(record.value()).append('\n');
                            actualCount++;
                        }
                    }

                    StringBuilder expected = new StringBuilder();
                    for (int i = 0; i < numItems; i++) {
                        expected.append(i).append('\n');
                    }
                    logger.info("number of committed items in the sink so far: " + actualCount);
                    assertEquals(expected.toString(), actualSinkContents.toString());
                    // if content matches, break the loop. Otherwise restart and try again
                    break;
                } catch (AssertionError e) {
                    if (NANOSECONDS.toSeconds(System.nanoTime() - start) >= 20) { // TODO [viliam] use 60
                        throw e;
                    }
                }
            }
        } finally {
            // We have to remove the job before bringing down Kafka broker because
            // the producer can get stuck otherwise.
            ditchJob(job, instances());
        }
    }

    @Test
    public void test_resumeTransaction() throws Exception {
        properties.put("transactional.id", "txn.resumeTransactionTest");

        // produce items
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        producer.initTransactions();
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(topic, 0, null, "0")).get();
        producer.send(new ProducerRecord<>(topic, 0, null, "1")).get();
        long producerId = ResumeTransactionUtil.getProducerId(producer);
        short epoch = ResumeTransactionUtil.getEpoch(producer);

        // close the producer
        producer.close();

        // verify items are not visible
        KafkaConsumer<String, String> consumer = kafkaTestSupport.createConsumer(topic);
        ConsumerRecords<String, String> polledRecords = consumer.poll(Duration.ofSeconds(2));
        assertEquals(0, polledRecords.count());

        // recover and commit
        producer = new KafkaProducer<>(properties);
        ResumeTransactionUtil.resumeTransaction(logger, producer, producerId, epoch,
                properties.getProperty("transactional.id"));
        producer.commitTransaction();

        // verify items are visible
        polledRecords = consumer.poll(Duration.ofSeconds(2));
        StringBuilder actualContents = new StringBuilder();
        for (ConsumerRecord<String, String> record : polledRecords) {
            actualContents.append(record.value()).append('\n');
        }
        assertEquals("0\n1\n", actualContents.toString());

        producer.close();
        consumer.close();
    }

    @Test
    public void when_transactionRolledBackHeuristically_then_sinkIgnoresIt() throws Exception {
        /*
        Design of the test:
        We'll create a processor, process 1 item and do phase-1 of the snapshot and then throw
        it away. Then we'll create a new processor and will try to restore the snapshot. It should
        try to commit the transaction from the previous processor, but that transaction timed out,
        which should be logged and ignored.
         */
        int txnTimeout = 2000;
        properties.setProperty("transaction.timeout.ms", String.valueOf(txnTimeout));
        Processor processor = WriteKafkaP.supplier(properties, o -> new ProducerRecord<>(topic, o), true).get();
        TestOutbox outbox = new TestOutbox(new int[0], 1024);
        TestProcessorContext procContext = new TestProcessorContext()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        processor.init(outbox, procContext);
        TestInbox inbox = new TestInbox();
        inbox.add("foo");
        processor.process(0, inbox);
        assertEquals("inbox size", 0, inbox.size());
        assertTrue(processor.saveToSnapshot());
        processor.close();

        inbox.addAll(outbox.snapshotQueue());

        // transaction.abort.timed.out.transaction.cleanup.interval.ms is set to 200, allow it to kick in
        sleepMillis(txnTimeout + 1000);

        // create the 2nd processor
        processor = WriteKafkaP.supplier(properties, o -> new ProducerRecord<>(topic, o), true).get();
        processor.init(outbox, procContext);
        processor.restoreFromSnapshot(inbox);
        processor.finishSnapshotRestore();
    }

    private void assertTopicContentsEventually(Map<String, String> expectedMap, boolean assertPartitionEqualsKey) {
        try (KafkaConsumer<String, String> consumer = kafkaTestSupport.createConsumer(topic)) {
            long timeLimit = System.nanoTime() + SECONDS.toNanos(10);
            for (int totalRecords = 0; totalRecords < expectedMap.size() && System.nanoTime() < timeLimit; ) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    assertEquals("key=" + record.key(), expectedMap.get(record.key()), record.value());
                    if (assertPartitionEqualsKey) {
                        assertEquals(Integer.parseInt(record.key()), record.partition());
                    }
                    totalRecords++;
                }
            }
        }
    }

    private static final class ProcessorWithEntryAndLatch extends AbstractProcessor {
        static volatile boolean isDone;
        static volatile boolean allowSnapshot;

        private Traverser<Entry<String, String>> t = Traversers.singleton(entry("k", "v"));

        private ProcessorWithEntryAndLatch() {
            // reset so that values from previous run don't remain
            isDone = false;
            allowSnapshot = false;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean saveToSnapshot() {
            return allowSnapshot;
        }

        @Override
        public boolean complete() {
            // emit the item and wait for the latch to complete
            return emitFromTraverser(t) && isDone;
        }
    }
}
