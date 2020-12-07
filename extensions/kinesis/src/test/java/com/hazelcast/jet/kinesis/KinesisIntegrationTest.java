/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.kinesis;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.MergeShardsRequest;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.SplitShardRequest;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.JobProxy;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.kinesis.impl.HashRange;
import com.hazelcast.jet.kinesis.impl.KinesisHelper;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.MapUtil.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.pipeline.test.Assertions.assertCollectedEventually;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
public class KinesisIntegrationTest extends JetTestSupport {

    //todo: tests are slow... why?

    @ClassRule
    public static final LocalStackContainer LOCALSTACK = new LocalStackContainer("0.12.1")
            .withServices(Service.KINESIS);

    private static final int KEYS = 10;
    private static final int MEMBER_COUNT = 2;
    private static final int MESSAGES = 25_000;
    private static final String STREAM = "TestStream";
    private static final String RESULTS = "Results";

    private static AwsConfig AWS_CONFIG;
    private static AmazonKinesisAsync KINESIS;
    private static KinesisHelper HELPER;

    private JetInstance[] cluster;
    private IMap<String, List<String>> results;

    @BeforeClass
    public static void beforeClass() {
        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
        //todo: force jackson versions to what we use (2.11.x) and have the resulting issue
        // fixed by Localstack (https://github.com/localstack/localstack/issues/3208)

        AWS_CONFIG = new AwsConfig(
                "http://" + LOCALSTACK.getHost() + ":" + LOCALSTACK.getMappedPort(4566),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey()
        );
        KINESIS = AWS_CONFIG.buildClient();
        HELPER = new KinesisHelper(KINESIS, STREAM, Logger.getLogger(KinesisIntegrationTest.class));
    }

    @AfterClass
    public static void afterClass() {
        KINESIS.shutdown();
    }

    @Before
    public void before() {
        JetConfig jetConfig = new JetConfig();
        jetConfig.getInstanceConfig().setCooperativeThreadCount(12); //todo: don't force this, let Jenkins be Jenkins
        cluster = createJetMembers(jetConfig, MEMBER_COUNT);
        results = jet().getMap(RESULTS);
    }

    @After
    public void after() {
        cleanUpCluster(cluster);

        deleteStream();
        HELPER.waitForStreamToDisappear();

        results.clear();
        results.destroy();
    }

    @Test
    @Category(SerialTest.class)
    public void timestampsAndWatermarks() {
        createStream(1);
        HELPER.waitForStreamToActivate();

        sendMessages(false);

        try {
            Pipeline pipeline = Pipeline.create();
            pipeline.readFrom(kinesisSource())
                    .withNativeTimestamps(0)
                    .window(WindowDefinition.sliding(500, 100))
                    .aggregate(counting())
                    .apply(assertCollectedEventually(ASSERT_TRUE_EVENTUALLY_TIMEOUT, windowResults -> {
                        assertTrue(windowResults.size() > 1); //multiple windows, so watermark works
                    }));

            jet().newJob(pipeline).join();
            fail("Expected exception not thrown");
        } catch (CompletionException ce) {
            Throwable cause = peel(ce);
            assertTrue(cause instanceof JetException);
            assertTrue(cause.getCause() instanceof AssertionCompletedException);
        }
    }

    @Test
    @Category(SerialTest.class)
    public void staticStream_1Shard() {
        staticStream(1);
    }

    @Test
    @Category({SerialTest.class, NightlyTest.class})
    public void staticStream_2Shards() {
        staticStream(2);
    }

    @Test
    @Category({SerialTest.class, NightlyTest.class})
    public void staticStream_50Shards() {
        staticStream(50);
    }

    private void staticStream(int shards) {
        createStream(shards);
        HELPER.waitForStreamToActivate();

        jet().newJob(getPipeline());

        Map<String, List<String>> expectedMessages = sendMessages(true);
        assertMessages(expectedMessages, true, false);
    }

    @Test
    @Category({SerialTest.class, NightlyTest.class})
    public void dynamicStream_2Shards_mergeBeforeData() {
        createStream(2);
        HELPER.waitForStreamToActivate();

        List<Shard> shards = listActiveShards();
        mergeShards(shards.get(0), shards.get(1));

        jet().newJob(getPipeline());

        Map<String, List<String>> expectedMessages = sendMessages(true);
        assertMessages(expectedMessages, true, false);
    }

    @Test
    @Category(SerialTest.class)
    public void dynamicStream_2Shards_mergeDuringData() {
        dynamicStream_mergesDuringData(2, 1);
    }

    @Test
    @Category({SerialTest.class, NightlyTest.class})
    public void dynamicStream_50Shards_mergesDuringData() {
        //important to test with more shards than can fit in a single list shards response
        dynamicStream_mergesDuringData(50, 5);
    }

    private void dynamicStream_mergesDuringData(int shards, int merges) {
        createStream(shards);
        HELPER.waitForStreamToActivate();

        jet().newJob(getPipeline());

        Map<String, List<String>> expectedMessages = sendMessages(false);

        //wait for some data to start coming out of the pipeline, before starting the merging
        assertTrueEventually(() -> assertFalse(results.isEmpty()));

        List<Shard> oldShards = Collections.emptyList();
        for (int i = 0; i < merges; i++) {
            Set<String> oldShardIds = oldShards.stream().map(Shard::getShardId).collect(Collectors.toSet());
            List<Shard> currentShards = listActiveShards();
            List<Shard> newShards = currentShards.stream()
                    .filter(shard -> !oldShardIds.contains(shard.getShardId()))
                    .collect(toList());
            assertTrue(newShards.size() >= 1);
            oldShards = currentShards;

            Collections.shuffle(newShards);
            Tuple2<Shard, Shard> adjacentPair = findAdjacentPair(newShards.get(0), currentShards);
            mergeShards(adjacentPair.f0(), adjacentPair.f1());
            HELPER.waitForStreamToActivate();
        }

        assertMessages(expectedMessages, false, false);
    }

    @Test
    @Category({SerialTest.class, NightlyTest.class})
    public void dynamicStream_1Shard_splitBeforeData() {
        createStream(1);
        HELPER.waitForStreamToActivate();

        List<Shard> shards = listActiveShards();
        splitShard(shards.get(0));

        jet().newJob(getPipeline());

        Map<String, List<String>> expectedMessages = sendMessages(true);
        assertMessages(expectedMessages, true, false);
    }

    @Test
    @Category(SerialTest.class)
    public void dynamicStream_1Shard_splitsDuringData() {
        dynamicStream_splitsDuringData(1, 3);
    }

    @Test
    @Category({SerialTest.class, NightlyTest.class})
    public void dynamicStream_10Shards_splitsDuringData() {
        dynamicStream_splitsDuringData(10, 10);
    }

    private void dynamicStream_splitsDuringData(int shards, int splits) {
        createStream(shards);
        HELPER.waitForStreamToActivate();

        jet().newJob(getPipeline());

        Map<String, List<String>> expectedMessages = sendMessages(false);

        //wait for some data to start coming out of the pipeline, before starting the splits
        assertTrueEventually(() -> assertFalse(results.isEmpty()));

        List<Shard> oldShards = Collections.emptyList();
        for (int i = 0; i < splits; i++) {
            Set<String> oldShardIds = oldShards.stream().map(Shard::getShardId).collect(Collectors.toSet());
            List<Shard> currentShards = listActiveShards();
            List<Shard> newShards = currentShards.stream()
                    .filter(shard -> !oldShardIds.contains(shard.getShardId()))
                    .collect(toList());
            assertTrue(newShards.size() >= 1);
            oldShards = currentShards;

            Collections.shuffle(newShards);
            splitShard(newShards.get(0));
            HELPER.waitForStreamToActivate();
        }

        assertMessages(expectedMessages, false, false);
    }

    @Test
    @Category(SerialTest.class)
    public void restart_staticStream_graceful() {
        restart_staticStream(true);
    }

    @Test
    @Category(SerialTest.class)
    public void restart_staticStream_non_graceful() {
        restart_staticStream(false);
    }

    private void restart_staticStream(boolean graceful) {
        createStream(3);
        HELPER.waitForStreamToActivate();

        JobConfig jobConfig = new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE)
                .setSnapshotIntervalMillis(SECONDS.toMillis(1));
        Job job = jet().newJob(getPipeline(), jobConfig);

        Map<String, List<String>> expectedMessages = sendMessages(false);

        //wait for some data to start coming out of the pipeline
        assertTrueEventually(() -> assertFalse(results.isEmpty()));

        ((JobProxy) job).restart(graceful);

        assertMessages(expectedMessages, true, !graceful);
    }

    @Test
    @Category(SerialTest.class)
    public void restart_dynamicStream_graceful() {
        restart_dynamicStream(true);
    }

    @Test
    @Category({SerialTest.class, NightlyTest.class})
    public void restart_dynamicStream_non_graceful() {
        restart_dynamicStream(false);
    }

    private void restart_dynamicStream(boolean graceful) {
        createStream(3);
        HELPER.waitForStreamToActivate();

        JobConfig jobConfig = new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE)
                .setSnapshotIntervalMillis(SECONDS.toMillis(1));
        Job job = jet().newJob(getPipeline(), jobConfig);

        Map<String, List<String>> expectedMessages = sendMessages(false);

        //wait for some data to start coming out of the pipeline
        assertTrueEventually(() -> assertFalse(results.isEmpty()));

        Shard shardToSplit = listActiveShards().get(1);
        splitShard(shardToSplit);

        HELPER.waitForStreamToActivate();

        List<Shard> shardsAfterSplit = listActiveShards();
        Tuple2<Shard, Shard> shardsToMerge = findAdjacentPair(shardsAfterSplit.get(0), shardsAfterSplit);
        mergeShards(shardsToMerge.f0(), shardsToMerge.f1());

        HELPER.waitForStreamToActivate();

        ((JobProxy) job).restart(graceful);

        assertMessages(expectedMessages, false, !graceful);
    }

    private Map<String, List<String>> sendMessages(boolean join) {
        List<Entry<String, String>> msgEntryList = IntStream.range(0, MESSAGES)
                .boxed()
                .map(i -> entry(Integer.toString(i % KEYS), i))
                .map(e -> entry(e.getKey(), String.format("%s: msg %09d", e.getKey(), e.getValue())))
                .collect(toList());

        BatchSource<Entry<String, byte[]>> source = TestSources.items(msgEntryList.stream()
                .map(e1 -> entry(e1.getKey(), e1.getValue().getBytes()))
                .collect(toList()));
        Sink<Entry<String, byte[]>> sink = KinesisSinks.kinesis(STREAM)
                .withEndpoint(AWS_CONFIG.getEndpoint())
                .withRegion(AWS_CONFIG.getRegion())
                .withCredentials(AWS_CONFIG.getAccessKey(), AWS_CONFIG.getSecretKey())
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .writeTo(sink);

        Job sendJob = jet().newJob(pipeline);

        Map<String, List<String>> retMap = toMap(msgEntryList);

        if (join) {
            sendJob.join();
        }

        return retMap;
    }

    private JetInstance jet() {
        return cluster[0];
    }

    private Pipeline getPipeline() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(kinesisSource())
                .withNativeTimestamps(0)
                .rebalance(Entry::getKey)
                .map(e -> entry(e.getKey(), Collections.singletonList(new String(e.getValue()))))
                .writeTo(Sinks.mapWithMerging(results, Entry::getKey, Entry::getValue, (l1, l2) -> {
                    ArrayList<String> list = new ArrayList<>();
                    list.addAll(l1);
                    list.addAll(l2);
                    return list;
                }));
        return pipeline;
    }

    private void assertMessages(Map<String, List<String>> expected, boolean checkOrder, boolean deduplicate) {
        assertTrueEventually(() -> {
            assertEquals(getKeySetsDifferDescription(expected, results), expected.keySet(), results.keySet());

            for (Entry<String, List<String>> entry : expected.entrySet()) {
                String key = entry.getKey();
                List<String> expectedMessages = entry.getValue();

                List<String> actualMessages = results.get(key);
                if (deduplicate) {
                    actualMessages = actualMessages.stream().distinct().collect(toList());
                }
                if (!checkOrder) {
                    actualMessages = new ArrayList<>(actualMessages);
                    actualMessages.sort(String::compareTo);
                }
                assertEquals(getMessagesDifferDescription(key, expectedMessages, actualMessages),
                        expectedMessages, actualMessages);
            }
        });
    }

    private static StreamSource<Entry<String, byte[]>> kinesisSource() {
        return KinesisSources.kinesis(STREAM)
                .withEndpoint(AWS_CONFIG.getEndpoint())
                .withRegion(AWS_CONFIG.getRegion())
                .withCredentials(AWS_CONFIG.getAccessKey(), AWS_CONFIG.getSecretKey())
                .build();
    }

    private static void createStream(int shardCount) {
        CreateStreamRequest request = new CreateStreamRequest();
        request.setShardCount(shardCount);
        request.setStreamName(STREAM);
        KINESIS.createStream(request);
    }

    private static void deleteStream() {
        KINESIS.deleteStream(STREAM);
        assertTrueEventually(() -> assertFalse(KINESIS.listStreams().isHasMoreStreams()));
    }

    private static void mergeShards(Shard shard1, Shard shard2) {
        MergeShardsRequest request = new MergeShardsRequest();
        request.setStreamName(STREAM);
        request.setShardToMerge(shard1.getShardId());
        request.setAdjacentShardToMerge(shard2.getShardId());

        System.out.println("Merging " + shard1.getShardId() + " with " + shard2.getShardId());
        KINESIS.mergeShards(request);
    }

    private static void splitShard(Shard shard) {
        HashRange range = HashRange.range(shard.getHashKeyRange());
        BigInteger middle = range.getMinInclusive().add(range.getMaxExclusive()).divide(BigInteger.valueOf(2));

        SplitShardRequest request = new SplitShardRequest();
        request.setStreamName(STREAM);
        request.setShardToSplit(shard.getShardId());
        request.setNewStartingHashKey(middle.toString());

        System.out.println("Splitting " + shard.getShardId());
        KINESIS.splitShard(request);
    }

    private static List<Shard> listActiveShards() {
        return HELPER.listShards(KinesisHelper::shardActive);
    }

    private static String getKeySetsDifferDescription(Map<String, List<String>> expected,
                                                      Map<String, List<String>> actual) {
        return "Key sets differ!" +
                "\n\texpected: " + new TreeSet<>(expected.keySet()) +
                "\n\t  actual: " + new TreeSet<>(actual.keySet());
    }

    private static String getMessagesDifferDescription(String key, List<String> expected, List<String> actual) {
        StringBuilder sb = new StringBuilder()
                .append("Messages for key ").append(key).append(" differ!")
                .append("\n\texpected: ").append(expected.size())
                .append("\n\t  actual: ").append(actual.size());

        for (int i = 0; i < min(expected.size(), actual.size()); i++) {
            if (!expected.get(i).equals(actual.get(i))) {
                sb.append("\n\tfirst difference at index: ").append(i);
                sb.append("\n\t\texpected: ");
                for (int j = max(0, i - 2); j < min(i + 5, expected.size()); j++) {
                    sb.append(j).append(": ").append(expected.get(j)).append(", ");
                }
                sb.append("\n\t\t  actual: ");
                for (int j = max(0, i - 2); j < min(i + 5, actual.size()); j++) {
                    sb.append(j).append(": ").append(actual.get(j)).append(", ");
                }
                break;
            }
        }

        return sb.toString();
    }

    private static Tuple2<Shard, Shard> findAdjacentPair(Shard shard, List<Shard> allShards) {
        HashRange shardRange = HashRange.range(shard.getHashKeyRange());
        for (Shard examinedShard : allShards) {
            HashRange examinedRange = HashRange.range(examinedShard.getHashKeyRange());
            if (shardRange.isAdjacent(examinedRange)) {
                if (shardRange.getMinInclusive().compareTo(examinedRange.getMinInclusive()) <= 0) {
                    return Tuple2.tuple2(shard, examinedShard);
                } else {
                    return Tuple2.tuple2(examinedShard, shard);
                }
            }
        }
        throw new IllegalStateException("There must be an adjacent shard");
    }

    private static Map<String, List<String>> toMap(List<Entry<String, String>> entryList) {
        return entryList.stream()
                .collect(Collectors.toMap(
                        Entry::getKey,
                        e -> Collections.singletonList(e.getValue()),
                        (l1, l2) -> {
                            ArrayList<String> retList = new ArrayList<>();
                            retList.addAll(l1);
                            retList.addAll(l2);
                            return retList;
                        }
                ));
    }

}
