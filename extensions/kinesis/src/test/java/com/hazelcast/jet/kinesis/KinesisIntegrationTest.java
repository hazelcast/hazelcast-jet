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
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.IMap;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;

import javax.annotation.Nonnull;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.MapUtil.entry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class KinesisIntegrationTest extends SimpleTestInClusterSupport {

    //todo: tests are slow... is it the sink?

    @ClassRule
    public static LocalStackContainer LOCALSTACK = new LocalStackContainer("0.12.1")
            .withServices(Service.KINESIS);

    private static final int KEYS = 10;
    private static final int MEMBER_COUNT = 2;

    @BeforeClass
    public static void beforeClass() {
        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
        //todo: force jackson versions to what we use (2.11.x) and have the resulting issue
        // fixed by Localstack (https://github.com/localstack/localstack/issues/3208)

        initialize(MEMBER_COUNT, null);
    }

    @Test
    public void staticStream_1Shard() throws Exception {
        staticStream(1);
    }

    @Test
    public void staticStream_2Shards() throws Exception {
        staticStream(2);
    }

    @Test
    public void staticStream_50Shards() throws Exception {
        staticStream(50);
    }

    private void staticStream(int shardCount) throws Exception {
        String stream = "static" + shardCount;

        IMap<String, List<String>> results = instance().getMap(stream);

        AwsConfig awsConfig = getAwsConfig(LOCALSTACK);
        AmazonKinesisAsync kinesis = awsConfig.buildClient();

        createStream(kinesis, stream, shardCount);
        waitForStreamToActivate(kinesis, stream);

        StreamSource<Entry<String, byte[]>> source = KinesisSources.kinesis(stream)
                .withEndpoint(awsConfig.getEndpoint())
                .withRegion(awsConfig.getRegion())
                .withCredentials(awsConfig.getAccessKey(), awsConfig.getSecretKey())
                .build();

        Sink<Entry<String, List<String>>> sink = Sinks.map(results);

        instance().newJob(getPipeline(source, sink));

        int messages = 25_000;
        Map<String, List<String>> expectedMessages = sendMessages(awsConfig, stream, messages, true);
        assertMessages(expectedMessages, results, true);
    }

    @Test
    @Ignore //todo
    public void dynamicStream_2Shards_mergeBeforeData() {
        fail("Not yet implemented"); //todo
    }

    @Test
    public void dynamicStream_2Shards_mergeDuringData() {
        int shardCount = 2;

        String stream = "dynamic" + shardCount + "Before";

        IMap<String, List<String>> results = instance().getMap(stream);

        AwsConfig awsConfig = getAwsConfig(LOCALSTACK);
        AmazonKinesisAsync kinesis = awsConfig.buildClient();

        createStream(kinesis, stream, shardCount);
        waitForStreamToActivate(kinesis, stream);

        StreamSource<Entry<String, byte[]>> source = KinesisSources.kinesis(stream)
                .withEndpoint(awsConfig.getEndpoint())
                .withRegion(awsConfig.getRegion())
                .withCredentials(awsConfig.getAccessKey(), awsConfig.getSecretKey())
                .build();

        Sink<Entry<String, List<String>>> sink = Sinks.map(results);

        instance().newJob(getPipeline(source, sink));

        int messages = 25_000;
        Map<String, List<String>> expectedMessages = sendMessages(awsConfig, stream, messages, false);

        List<String> shards = getShards(kinesis, stream);

        assertTrueEventually(() -> assertFalse(results.isEmpty()));
        mergeShards(kinesis, stream, shards.get(0), shards.get(1));

        assertMessages(expectedMessages, results, false);
    }

    @Test
    @Ignore //todo
    public void dynamicStream_10Shards_someMergesBeforeData() {
        fail("Not yet implemented"); //todo
    }

    @Test
    @Ignore //todo
    public void dynamicStream_10Shards_someMergesDuringData() {
        fail("Not yet implemented"); //todo
    }

    @Test
    @Ignore //todo
    public void dynamicStream_50Shards_allMergeDuringData() {
        fail("Not yet implemented"); //todo
    }

    //todo: test with more than 100 shards, monitor behaves differently in that case

    private static AwsConfig getAwsConfig(LocalStackContainer localstack) {
        return new AwsConfig(
                "http://localhost:" + localstack.getMappedPort(4566),
                localstack.getRegion(),
                localstack.getAccessKey(),
                localstack.getSecretKey()
        );
    }

    private static void createStream(AmazonKinesisAsync kinesis, String streamName, int shardCount) {
        CreateStreamRequest request = new CreateStreamRequest();
        request.setShardCount(shardCount);
        request.setStreamName(streamName);
        kinesis.createStream(request);
    }

    private static Pipeline getPipeline(StreamSource<Entry<String, byte[]>> source,
                                        Sink<Entry<String, List<String>>> sink) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withoutTimestamps()
                .groupingKey(Entry::getKey)
                .mapStateful((SupplierEx<List<String>>) ArrayList::new,
                        (s, k, e) -> {
                            String m = new String(e.getValue(), Charset.defaultCharset());
                            s.add(m);
                            return Util.<String, List<String>>entry(k, new ArrayList<>(s));
                        })
                .writeTo(sink);
        return pipeline;
    }

    private static Map<String, List<String>> sendMessages(AwsConfig awsConfig, String stream, int count, boolean join) {
        List<Entry<String, String>> msgEntryList = IntStream.range(0, count)
                .boxed()
                .map(i -> entry(Integer.toString(i % KEYS), i))
                .map(e -> entry(e.getKey(), String.format("Message %09d for key %s", e.getValue(), e.getKey())))
                .collect(Collectors.toList());

        BatchSource<Entry<String, byte[]>> source = TestSources.items(msgEntryList.stream()
                .map(e1 -> entry(e1.getKey(), e1.getValue().getBytes()))
                .collect(Collectors.toList()));
        Sink<Entry<String, byte[]>> sink = KinesisSinks.kinesis(stream)
                .withEndpoint(awsConfig.getEndpoint())
                .withRegion(awsConfig.getRegion())
                .withCredentials(awsConfig.getAccessKey(), awsConfig.getSecretKey())
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .writeTo(sink);

        Job job = instance().newJob(pipeline);

        Map<String, List<String>> retMap = toMap(msgEntryList);

        if (join) {
            job.join();
        }

        return retMap;
    }

    private static void waitForStreamToActivate(AmazonKinesisAsync kinesis, String stream) {
        while (true) {
            StreamDescription description = describeStream(kinesis, stream);
            String status = description.getStreamStatus();
            if ("ACTIVE".equals(status)) {
                return;
            } else {
                System.out.println("Waiting for stream " + stream + " to be active ...");
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static StreamDescription describeStream(AmazonKinesisAsync kinesis, String stream) {
        return kinesis.describeStream(stream).getStreamDescription();
    }

    private static List<String> getShards(AmazonKinesisAsync kinesis, String stream) {
        return describeStream(kinesis, stream).getShards().stream()
                .map(Shard::getShardId)
                .collect(Collectors.toList());
    }

    private static void mergeShards(AmazonKinesisAsync kinesis, String stream, String shard1, String shard2) {
        MergeShardsRequest request = new MergeShardsRequest();
        request.setStreamName(stream);
        request.setShardToMerge(shard1);
        request.setAdjacentShardToMerge(shard2);

        kinesis.mergeShards(request);
    }

    private static void assertMessages(
            Map<String, List<String>> expected,
            IMap<String, List<String>> actual,
            boolean checkOrder
    ) {
        assertTrueEventually(() -> {
            assertEquals(expected.keySet(), actual.keySet());

            for (Entry<String, List<String>> entry : expected.entrySet()) {
                String key = entry.getKey();
                List<String> expectedMessages = entry.getValue();

                List<String> actualMessages = actual.get(key);
                if (!checkOrder) {
                    actualMessages = new ArrayList<>(actualMessages);
                    actualMessages.sort(String::compareTo);
                }
                assertEquals(expectedMessages, actualMessages);
            }
        });
    }

    @Nonnull
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
