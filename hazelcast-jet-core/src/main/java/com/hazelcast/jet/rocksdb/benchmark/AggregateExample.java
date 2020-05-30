package com.hazelcast.jet.rocksdb.benchmark;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;

import java.util.stream.LongStream;

import static com.hazelcast.jet.aggregate.AggregateOperations.toList;


public class AggregateExample {

    private static final long RANGE = 100_000;
    private static final long NUM_KEYS = 100;

    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();
        Pipeline p = Pipeline.create();

        p.readFrom(longSource())
         .groupingKey(n -> n % NUM_KEYS)
         .aggregate(toList())
         .writeTo(Sinks.logger());

        jet.newJob(p).join();
    }

    private static BatchSource<Long> longSource() {
        return SourceBuilder
                .batch("longs", c -> LongStream.range(0, RANGE).boxed().iterator())
                .<Long>fillBufferFn((longs, buf) -> {
                    for (int i = 0; i < 128 && longs.hasNext(); i++) {
                        buf.add(longs.next());
                    }
                    if (!longs.hasNext()) {
                        buf.close();
                    }
                })
                .build();
    }
}
