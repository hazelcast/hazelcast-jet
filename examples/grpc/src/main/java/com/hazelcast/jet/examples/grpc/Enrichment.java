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

package com.hazelcast.jet.examples.grpc;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.examples.grpc.EnrichmentServiceGrpc.EnrichmentServiceFutureStub;
import com.hazelcast.jet.examples.grpc.datamodel.Broker;
import com.hazelcast.jet.examples.grpc.datamodel.Product;
import com.hazelcast.jet.examples.grpc.datamodel.Trade;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.function.Functions.entryValue;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_CURRENT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;

/**
 * Demonstrates the usage of the Pipeline API to enrich a data stream. We
 * generate a stream of stock trade events and each event has an associated
 * product ID and broker ID. The reference lists of products and brokers
 * are stored in files. The goal is to enrich the trades with the actual
 * name of the products and the brokers.
 * <p>
 * This example shows different ways of achieving this goal:
 * <ol>
 *     <li>Using Hazelcast {@code IMap}</li>
 *     <li>Using Hazelcast {@code ReplicatedMap}</li>
 *     <li>Using an external service (gRPC in this sample)</li>
 *     <li>Using the pipeline {@code hashJoin} operation</li>
 * </ol>
 * <p>
 * The details of each approach are documented with the associated method.
 * <p>
 * We generate the stream of trade events by updating a single key in the
 * {@code trades} map which has the Event Journal enabled. The event
 * journal emits a stream of update events.
 */
public final class Enrichment {

    private static final String TRADES = "trades";
    private static final String PRODUCTS = "products";
    private static final String BROKERS = "brokers";

    private final JetInstance jet;


    private Enrichment(JetInstance jet) {
        this.jet = jet;
    }

    /**
     * Builds a pipeline which enriches the stream with the response from a
     * gRPC service.
     * <p>
     * It starts a gRPC server that will provide product and broker names based
     * on an ID. The job then enriches incoming trades using the service. This
     * sample demonstrates a way to call external service with an async API
     * using the {@link GeneralStage#mapUsingContextAsync mapUsingContextAsync}
     * method.
     */
    private static Pipeline enrichUsingGRPC() throws Exception {
        Map<Integer, Product> productMap = readLines("products.txt")
                .collect(toMap(Entry::getKey, e -> new Product(e.getKey(), e.getValue())));
        Map<Integer, Broker> brokerMap = readLines("brokers.txt")
                .collect(toMap(Entry::getKey, e -> new Broker(e.getKey(), e.getValue())));

        int port = 50051;
        ServerBuilder.forPort(port)
                     .addService(new EnrichmentServiceImpl(productMap, brokerMap))
                     .build()
                     .start();
        System.out.println("*** Server started, listening on " + port);

        // The stream to be enriched: trades
        Pipeline p = Pipeline.create();
        StreamStage<Trade> trades = p
                .drawFrom(Sources.<Object, Trade>mapJournal(TRADES, START_FROM_CURRENT))
                .withoutTimestamps()
                .map(entryValue());

        // The context factory is the same for both enrichment steps
        ContextFactory<EnrichmentServiceFutureStub> contextFactory = ContextFactory
                .withCreateFn(x -> {
                    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port)
                                                                  .usePlaintext().build();
                    return EnrichmentServiceGrpc.newFutureStub(channel);
                })
                .withDestroyFn(stub -> {
                    ManagedChannel channel = (ManagedChannel) stub.getChannel();
                    channel.shutdown().awaitTermination(5, SECONDS);
                });

        // Enrich the trade by querying the product and broker name from the gRPC service
        trades
                .mapUsingContextAsync(contextFactory,
                        (stub, t) -> {
                            ProductInfoRequest request = ProductInfoRequest.newBuilder().setId(t.productId()).build();
                            return toCompletableFuture(stub.productInfo(request))
                                    .thenApply(productReply -> tuple2(t, productReply.getProductName()));
                        })
                .mapUsingContextAsync(contextFactory,
                        (stub, t) -> {
                            BrokerInfoRequest request = BrokerInfoRequest.newBuilder().setId(t.f0().brokerId()).build();
                            return toCompletableFuture(stub.brokerInfo(request))
                                    .thenApply(brokerReply -> tuple3(t.f0(), t.f1(), brokerReply.getBrokerName()));
                        })
                .drainTo(Sinks.logger());

        return p;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");

        JetConfig cfg = new JetConfig();
        cfg.getHazelcastConfig().getMapEventJournalConfig(TRADES).setEnabled(true);
        JetInstance jet = Jet.newJetInstance(cfg);
        Jet.newJetInstance(cfg);

        new Enrichment(jet).go();
    }

    private void go() throws Exception {
        EventGenerator eventGenerator = new EventGenerator(jet.getMap(TRADES));
        eventGenerator.start();
        try {
            // comment out the code to try the appropriate enrichment method
            Pipeline p = enrichUsingGRPC();
            Job job = jet.newJob(p);
            eventGenerator.generateEventsForFiveSeconds();
            job.cancel();
            try {
                job.join();
            } catch (CancellationException ignored) {
            }
        } finally {
            eventGenerator.shutdown();
            Jet.shutdownAll();
        }
    }

    private static Stream<Map.Entry<Integer, String>> readLines(String file) {
        try {
            return Files.lines(Paths.get(Enrichment.class.getResource(file).toURI()))
                    .map(Enrichment::splitLine);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Map.Entry<Integer, String> splitLine(String e) {
        int commaPos = e.indexOf(',');
        return entry(Integer.valueOf(e.substring(0, commaPos)), e.substring(commaPos + 1));
    }

    /**
     * Adapt a {@link ListenableFuture} to java standard {@link
     * CompletableFuture}, which is used by Jet.
     */
    private static <T> CompletableFuture<T> toCompletableFuture(ListenableFuture<T> lf) {
        CompletableFuture<T> f = new CompletableFuture<>();
        // note that we don't handle CompletableFuture.cancel()
        Futures.addCallback(lf, new FutureCallback<T>() {
            @Override
            public void onSuccess(@NullableDecl T result) {
                f.complete(result);
            }

            @Override
            public void onFailure(Throwable t) {
                f.completeExceptionally(t);
            }
        });
        return f;
    }
}
