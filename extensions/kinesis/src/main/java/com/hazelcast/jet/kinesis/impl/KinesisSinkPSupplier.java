package com.hazelcast.jet.kinesis.impl;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class KinesisSinkPSupplier<T> implements ProcessorSupplier {

    private static final long serialVersionUID = 1L;

    @Nonnull
    private final AwsConfig awsConfig;
    @Nonnull
    private final String stream;
    @Nonnull
    private final FunctionEx<T, String> keyFn;
    @Nonnull
    private final FunctionEx<T, byte[]> valueFn;

    private transient AmazonKinesisAsync kinesis;

    public KinesisSinkPSupplier(
            @Nonnull AwsConfig awsConfig,
            @Nonnull String stream,
            @Nonnull FunctionEx<T, String> keyFn,
            @Nonnull FunctionEx<T, byte[]> valueFn
    ) {
        this.awsConfig = awsConfig;
        this.stream = stream;
        this.keyFn = keyFn;
        this.valueFn = valueFn;
    }

    @Override
    public void init(@Nonnull Context context) {
        this.kinesis = awsConfig.buildClient();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> new KinesisSinkP<T>(kinesis, stream, keyFn, valueFn))
                .collect(toList());
    }
}
