package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Processor which exploits natural batching of {@link Inbox} items.
 * For each received batch of items, emits all the items from the
 * traverser returned by the given list-of-items-to-traverser function.
 *
 * @param <T> received item type
 * @param <R> emitted item type
 */
public class TransformBatchedP<T, R> extends AbstractProcessor {

    private final int maxBatchSize;
    private final Function<List<? super T>, ? extends Traverser<? extends R>> mapper;

    private Traverser<? extends R> outputTraverser;

    public TransformBatchedP(
            int maxBatchSize,
            Function<List<? super T>, ? extends Traverser<? extends R>> mapper
    ) {
        this.maxBatchSize = maxBatchSize;
        this.mapper = mapper;
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (outputTraverser == null) {
            List<T> batch = new ArrayList<>(Math.min(inbox.size(), maxBatchSize));
            inbox.drainTo(batch, maxBatchSize);
            outputTraverser = mapper.apply(batch);
        }

        if (emitFromTraverser(outputTraverser)) {
            outputTraverser = null;
        }
    }
}
