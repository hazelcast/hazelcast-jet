package com.hazelcast.jet.examples.protobuf;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.protobuf.Messages.Product;
import com.hazelcast.jet.protobuf.ProtobufSerializerHook;

/**
 * Demonstrates the usage of Protobuf serializer hook adapter.
 * <p>
 * {@link ProductSerializerHook} is discovered & registered via
 * 'META-INF/services/com.hazelcast.SerializerHook' and then used to
 * serialize local {@link com.hazelcast.collection.IList} items.
 */
public class ProtobufSerializerHookAdapter {

    private static final String LIST_NAME = "products";

    private JetInstance jet;

    public static void main(String[] args) {
        new ProtobufSerializerHookAdapter().go();
    }

    private void go() {
        try {
            setup();

            jet.newJob(buildPipeline()).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    private void setup() {
        jet = Jet.bootstrappedInstance();
    }

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        p.readFrom(TestSources.items("jam", "marmalade"))
         .map(name -> Product.newBuilder().setName(name).build())
         .writeTo(Sinks.list(LIST_NAME));

        return p;
    }

    @SuppressWarnings("unused")
    private static class ProductSerializerHook extends ProtobufSerializerHook<Product> {

        private static final int TYPE_ID = 17;

        ProductSerializerHook() {
            super(Product.class, TYPE_ID);
        }
    }
}
