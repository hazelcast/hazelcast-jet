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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class KinesisIntegrationTest {

    @Rule
    public LocalStackContainer localstack = new LocalStackContainer("latest")
            .withServices(Service.KINESIS);

    @BeforeClass
    public static void beforeClass() {
        // See https://github.com/mhart/kinesalite#cbor-protocol-issues-with-the-java-sdk
        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
    }

    @Test
    public void test() throws Exception {
        Integer port = localstack.getMappedPort(4566);
        System.out.println("port = " + port); //todo: remove

        String region = localstack.getRegion();
        System.out.println("region = " + region); //todo: remove

        String accessKey = localstack.getAccessKey();
        System.out.println("accessKey = " + accessKey); //todo: remove

        String secretKey = localstack.getSecretKey();
        System.out.println("secretKey = " + secretKey); //todo: remove

        AmazonKinesisAsync kinesis = amazonKinesis("http://localhost:" + port, region, accessKey, secretKey);

        String streamName = "StockTradeStream";
        createStream(kinesis, streamName, 1);
        StreamDescription streamDescription = waitForStreamStatus(kinesis, streamName, "ACTIVE");

        String streamARN = streamDescription.getStreamARN();
        System.out.println("streamARN = " + streamARN); //todo: remove

        List<Shard> shards = streamDescription.getShards();
        System.out.println("shards.size() = " + shards.size()); //todo: remove
        if (shards.isEmpty()) {
            return;
        }

        String shardIterator = getShardIterator(kinesis, streamName, shards.get(0).getShardId(), "LATEST");
        System.out.println("shardIterator = " + shardIterator);

        new Thread(
                () -> {
                    StockTradeGenerator stockTradeGenerator = new StockTradeGenerator();
                    while (true) {
                        StockTrade trade = stockTradeGenerator.getRandomTrade();
                        ByteBuffer data = ByteBuffer.wrap(trade.toJsonAsBytes());
                        getPutRecordRequest(kinesis, streamName, trade.getTickerSymbol(), data);
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
        ).start();

        while (true) {
            GetRecordsResult result = getRecords(kinesis, shardIterator);
            List<Record> records = result.getRecords();
            System.out.println("Got " + records.size() + " records"); //todo: remove
            shardIterator = result.getNextShardIterator();

            TimeUnit.SECONDS.sleep(1);
        }

    }

    private static AmazonKinesisAsync amazonKinesis(String endpoint, String region, String accessKey, String secretKey) {
        return AmazonKinesisAsyncClientBuilder.standard()
                .withClientConfiguration(
                        new ClientConfiguration()
                                .withMaxErrorRetry(0)
                                .withConnectionTimeout(1000))
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .build();
    }

    private static void createStream(AmazonKinesisAsync kinesis, String streamName, int shardCount) {
        CreateStreamRequest request = new CreateStreamRequest();
        request.setShardCount(shardCount);
        request.setStreamName(streamName);
        kinesis.createStream(request);
    }

    private static StreamDescription waitForStreamStatus(AmazonKinesisAsync kinesis, String stream, String targetStatus) {
        while (true) {
            StreamDescription description = kinesis.describeStream(stream).getStreamDescription();
            String status = description.getStreamStatus();
            if (targetStatus.equals(status)) {
                return description;
            } else {
                System.out.println("Waiting for stream " + stream + " to be " + targetStatus + " ..."); //todo: remove
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static String getShardIterator(AmazonKinesisAsync kinesis, String steram, String id, String type) {
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setStreamName(steram);
        getShardIteratorRequest.setShardId(id);
        getShardIteratorRequest.setShardIteratorType(type);

        GetShardIteratorResult getShardIteratorResult = kinesis.getShardIterator(getShardIteratorRequest);
        return getShardIteratorResult.getShardIterator();
    }

    private static GetRecordsResult getRecords(AmazonKinesisAsync kinesis, String shardIterator) {
        GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        getRecordsRequest.setShardIterator(shardIterator);

        return kinesis.getRecords(getRecordsRequest);
    }

    private static PutRecordResult getPutRecordRequest(AmazonKinesisAsync kinesis, String stream, String key,
                                                   ByteBuffer data) {
        PutRecordRequest request = new PutRecordRequest();
        request.setPartitionKey(key);
        request.setData(data);
        request.setStreamName(stream);

        return kinesis.putRecord(request);
    }

    public static class StockTradeGenerator {

        private static final List<StockPrice> STOCK_PRICES = new ArrayList<StockPrice>();
        static {
            STOCK_PRICES.add(new StockPrice("AAPL", 119.72));
            STOCK_PRICES.add(new StockPrice("XOM", 91.56));
            STOCK_PRICES.add(new StockPrice("GOOG", 527.83));
            STOCK_PRICES.add(new StockPrice("BRK.A", 223999.88));
            STOCK_PRICES.add(new StockPrice("MSFT", 42.36));
            STOCK_PRICES.add(new StockPrice("WFC", 54.21));
            STOCK_PRICES.add(new StockPrice("JNJ", 99.78));
            STOCK_PRICES.add(new StockPrice("WMT", 85.91));
            STOCK_PRICES.add(new StockPrice("CHL", 66.96));
            STOCK_PRICES.add(new StockPrice("GE", 24.64));
            STOCK_PRICES.add(new StockPrice("NVS", 102.46));
            STOCK_PRICES.add(new StockPrice("PG", 85.05));
            STOCK_PRICES.add(new StockPrice("JPM", 57.82));
            STOCK_PRICES.add(new StockPrice("RDS.A", 66.72));
            STOCK_PRICES.add(new StockPrice("CVX", 110.43));
            STOCK_PRICES.add(new StockPrice("PFE", 33.07));
            STOCK_PRICES.add(new StockPrice("FB", 74.44));
            STOCK_PRICES.add(new StockPrice("VZ", 49.09));
            STOCK_PRICES.add(new StockPrice("PTR", 111.08));
            STOCK_PRICES.add(new StockPrice("BUD", 120.39));
        }

        /** The ratio of the deviation from the mean price **/
        private static final double MAX_DEVIATION = 0.2; // ie 20%

        /** The number of shares is picked randomly between 1 and the MAX_QUANTITY **/
        private static final int MAX_QUANTITY = 10000;

        /** Probability of trade being a sell **/
        private static final double PROBABILITY_SELL = 0.4; // ie 40%

        private final Random random = new Random();
        private AtomicLong id = new AtomicLong(1);

        /**
         * Return a random stock trade with a unique id every time.
         *
         */
        public StockTrade getRandomTrade() {
            // pick a random stock
            StockPrice stockPrice = STOCK_PRICES.get(random.nextInt(STOCK_PRICES.size()));
            // pick a random deviation between -MAX_DEVIATION and +MAX_DEVIATION
            double deviation = (random.nextDouble() - 0.5) * 2.0 * MAX_DEVIATION;
            // set the price using the deviation and mean price
            double price = stockPrice.price * (1 + deviation);
            // round price to 2 decimal places
            price = Math.round(price * 100.0) / 100.0;

            // set the trade type to buy or sell depending on the probability of sell
            StockTrade.TradeType tradeType = StockTrade.TradeType.BUY;
            if (random.nextDouble() < PROBABILITY_SELL) {
                tradeType = StockTrade.TradeType.SELL;
            }

            // randomly pick a quantity of shares
            long quantity = random.nextInt(MAX_QUANTITY) + 1; // add 1 because nextInt() will return between 0 (inclusive)
            // and MAX_QUANTITY (exclusive). we want at least 1 share.

            return new StockTrade(stockPrice.tickerSymbol, tradeType, price, quantity, id.getAndIncrement());
        }

        private static class StockPrice {
            String tickerSymbol;
            double price;

            StockPrice(String tickerSymbol, double price) {
                this.tickerSymbol = tickerSymbol;
                this.price = price;
            }
        }

    }

    public static class StockTrade {

        private static final ObjectMapper JSON = new ObjectMapper();
        static {
            JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }

        /**
         * Represents the type of the stock trade eg buy or sell.
         */
        public enum TradeType {
            BUY,
            SELL
        }

        private String tickerSymbol;
        private TradeType tradeType;
        private double price;
        private long quantity;
        private long id;

        public StockTrade(String tickerSymbol, TradeType tradeType, double price, long quantity, long id) {
            this.tickerSymbol = tickerSymbol;
            this.tradeType = tradeType;
            this.price = price;
            this.quantity = quantity;
            this.id = id;
        }

        public String getTickerSymbol() {
            return tickerSymbol;
        }

        public TradeType getTradeType() {
            return tradeType;
        }

        public double getPrice() {
            return price;
        }

        public long getQuantity() {
            return quantity;
        }

        public long getId() {
            return id;
        }

        public byte[] toJsonAsBytes() {
            try {
                return JSON.writeValueAsBytes(this);
            } catch (IOException e) {
                return null;
            }
        }

        public static StockTrade fromJsonAsBytes(byte[] bytes) {
            try {
                return JSON.readValue(bytes, StockTrade.class);
            } catch (IOException e) {
                return null;
            }
        }

        @Override
        public String toString() {
            return String.format("ID %d: %s %d shares of %s for $%.02f",
                    id, tradeType, quantity, tickerSymbol, price);
        }

    }

}
