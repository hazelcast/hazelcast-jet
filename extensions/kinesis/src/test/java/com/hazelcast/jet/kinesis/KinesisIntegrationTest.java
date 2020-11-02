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
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.kinesis.impl.KinesisSourcePMetaSupplier;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.IMap;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.Util.entry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class KinesisIntegrationTest extends SimpleTestInClusterSupport {

    private static final int KEYS = 10;
    private static final int MEMBER_COUNT = 1; //todo: 2

    @Rule
    public LocalStackContainer localstack = new LocalStackContainer("0.12.1")
            .withServices(Service.KINESIS);

    @BeforeClass
    public static void beforeClass() {
        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true"); //todo: performance issue...

        initialize(MEMBER_COUNT, null);
    }

    @Test
    public void test() {
        String testId = "test"; //todo: improve

        IMap<String, List<String>> results = instance().getMap(testId);

        AwsConfig awsConfig = new AwsConfig(
                "http://localhost:" + localstack.getMappedPort(4566),
                localstack.getRegion(),
                localstack.getAccessKey(),
                localstack.getSecretKey()
        );
        System.out.println(awsConfig);

        AmazonKinesisAsync kinesis = awsConfig.buildClient();

        String streamName = "StockTradeStream";
        createStream(kinesis, streamName, 1);
        waitForStreamToActivate(kinesis, streamName);

        Pipeline pipeline = Pipeline.create();
        StreamSource<Record> source = Sources.streamFromProcessor("source",
                new KinesisSourcePMetaSupplier(awsConfig, streamName)); //todo: source class
        pipeline.readFrom(source)
                .withoutTimestamps()
                .groupingKey(Record::getPartitionKey)
                .mapStateful((SupplierEx<List<String>>) ArrayList::new,
                        (s, k, r) -> {
                            s.add(new String(r.getData().array(), Charset.defaultCharset()));
                            return entry(k, new ArrayList<>(s));
                        })
                .writeTo(Sinks.map(results));
        instance().newJob(pipeline);

        int messages = 100;
        Map<String, List<String>> expectedMessages = sendMessages(kinesis, streamName, messages);
        assertTrueEventually(() -> assertEquals(expectedMessages, results));

    }

    private static void createStream(AmazonKinesisAsync kinesis, String streamName, int shardCount) {
        CreateStreamRequest request = new CreateStreamRequest();
        request.setShardCount(shardCount);
        request.setStreamName(streamName);
        kinesis.createStream(request);
    }

    private static Map<String, List<String>> sendMessages(AmazonKinesisAsync kinesis, String streamName, int messages) {
        List<PutRecordsRequestEntry> requestEntries = new ArrayList<>(messages); //todo: 500 is the maximum limit
        Map<String, List<String>> sentMessages = new HashMap<>();

        for (int i = 0; i < messages; i++) {
            String key = Integer.toString(i % KEYS);
            String message = "Message " + i + " for key " + key;
            sentMessages.merge(key, Collections.singletonList(message), (l1, l2) -> {
                ArrayList<String> retList = new ArrayList<>();
                retList.addAll(l1);
                retList.addAll(l2);
                return retList;
            });
            requestEntries.add(putRecordRequestEntry(key, ByteBuffer.wrap(message.getBytes())));
        }

        PutRecordsResult response = kinesis.putRecords(putRecordsRequest(streamName, requestEntries));
        int failures = response.getFailedRecordCount();
        if (failures > 0) {
            fail("Sending messages to Kinesis failed: " + response);
        }

        return sentMessages;
    }

    private static void waitForStreamToActivate(AmazonKinesisAsync kinesis, String stream) {
        while (true) {
            StreamDescription description = kinesis.describeStream(stream).getStreamDescription();
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

    private static PutRecordsRequestEntry putRecordRequestEntry(String key, ByteBuffer data) {
        PutRecordsRequestEntry request = new PutRecordsRequestEntry();
        request.setPartitionKey(key);
        request.setData(data);
        return request;
    }

    private static PutRecordsRequest putRecordsRequest(String stream, Collection<PutRecordsRequestEntry> entries) {
        PutRecordsRequest request = new PutRecordsRequest();
        request.setRecords(entries);
        request.setStreamName(stream);
        return request;
    }

}
