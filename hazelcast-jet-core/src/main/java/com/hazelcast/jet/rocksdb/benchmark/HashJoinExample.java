package com.hazelcast.jet.rocksdb.benchmark;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;

import java.util.stream.LongStream;

public class HashJoinExample {
    private static final long RANGE = 100_000;
    private static final long NUM_KEYS = 100;
    private static final long SHIFT = 100;

    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();
        Pipeline p = Pipeline.create();

        BatchStage<Long> left = p.readFrom(longSource());
        BatchStage<Long> right = p.readFrom(longSource()).map(n -> n + SHIFT);

        left.innerHashJoin(right, JoinClause.onKeys(n -> n % NUM_KEYS, n -> n % NUM_KEYS),
                Tuple2::tuple2)
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
