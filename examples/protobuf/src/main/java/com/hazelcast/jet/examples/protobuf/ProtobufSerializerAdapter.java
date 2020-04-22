package com.hazelcast.jet.examples.protobuf;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.protobuf.Messages.Broker;
import com.hazelcast.jet.protobuf.ProtobufSerializer;

/**
 * Demonstrates the usage of Protobuf serializer adapter.
 * <p>
 * {@link BrokerSerializer} is registered on job level and then used to
 * serialize local {@link com.hazelcast.collection.IList} items.
 */
public class ProtobufSerializerAdapter {

    private static final String LIST_NAME = "brokers";

    private JetInstance jet;

    public static void main(String[] args) {
        new ProtobufSerializerAdapter().go();
    }

    private void go() {
        try {
            setup();

            JobConfig config = new JobConfig()
                    .registerSerializer(Broker.class, BrokerSerializer.class);
            jet.newJob(buildPipeline(), config).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    private void setup() {
        jet = Jet.bootstrappedInstance();
    }

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        p.readFrom(TestSources.items(1, 2, 3, 4, 5))
         .map(id -> Broker.newBuilder().setId(id).build())
         .writeTo(Sinks.list(LIST_NAME));

        return p;
    }

    @SuppressWarnings("unused")
    private static class BrokerSerializer extends ProtobufSerializer<Broker> {

        private static final int TYPED_ID = 13;

        BrokerSerializer() {
            super(Broker.class, TYPED_ID);
        }
    }
}
