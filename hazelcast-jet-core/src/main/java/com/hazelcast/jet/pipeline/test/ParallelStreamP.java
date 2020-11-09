package com.hazelcast.jet.pipeline.test;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;

import javax.annotation.Nonnull;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;


/**
 * @param <T> generated item type
 */
public class ParallelStreamP<T> extends AbstractProcessor {

    private static final long NANOS_PER_SECOND = SECONDS.toNanos(1);

    private final long nanoTimeMillisToCurrentTimeMillis = determineTimeOffset();
    private final long periodNanos;
    private final EventTimeMapper<? super T> eventTimeMapper;
    private long startNanoTime;
    private long globalProcessorIndex;
    private long totalParallelism;

    private long nowNanoTime;

    private long sequence;
    private Traverser<Object> traverser = new AppendableTraverser<>(2);

    private final List<GeneratorFunction<? extends T>> generators;
    private GeneratorFunction<? extends T> generator;

    ParallelStreamP(long eventsPerSecond, EventTimePolicy<? super T> eventTimePolicy, List<GeneratorFunction<? extends T>> generators) {
        this.startNanoTime = System.currentTimeMillis(); // temporarily holds the parameter value until init
        this.periodNanos = NANOS_PER_SECOND/ eventsPerSecond;
        this.eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        eventTimeMapper.addPartitions(1);
        this.generators = generators;
    }

    @Override
    protected void init(@Nonnull Context context) {
        totalParallelism = context.totalParallelism();
        globalProcessorIndex = context.localProcessorIndex();
        startNanoTime = MILLISECONDS.toNanos(startNanoTime + nanoTimeMillisToCurrentTimeMillis) +
                globalProcessorIndex * periodNanos;
        generator = generators.get((int) globalProcessorIndex);
    }

    @Override
    public boolean complete() {
        nowNanoTime = System.nanoTime();
        try {
            emitEvents();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private void emitEvents() throws Exception {
        long emitUpTo = (nowNanoTime - startNanoTime) / periodNanos;
        while (emitFromTraverser(traverser) && sequence * totalParallelism < emitUpTo) {
            long timestampNanoTime = startNanoTime + sequence * totalParallelism * periodNanos;
            long timestamp = NANOSECONDS.toMillis(timestampNanoTime) - nanoTimeMillisToCurrentTimeMillis;
            T item = generator.generate(timestamp, sequence++);
            traverser = eventTimeMapper.flatMapEvent(nowNanoTime, item, 0, timestamp);
        }
    }

    private static long determineTimeOffset() {
        return NANOSECONDS.toMillis(System.nanoTime()) - System.currentTimeMillis();
    }

}
