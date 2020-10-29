package com.hazelcast.jet.kinesis.impl;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class KinesisSourcePSupplier implements ProcessorSupplier {

    private final AwsConfig awsConfig;
    private final String stream;
    private final HashRange hashRange;

    private transient AmazonKinesisAsync kinesis;

    public KinesisSourcePSupplier(
            @Nonnull AwsConfig awsConfig,
            @Nonnull String stream,
            @Nonnull HashRange hashRange
    ) {
        this.awsConfig = awsConfig;
        this.stream = stream;
        this.hashRange = hashRange;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        this.kinesis = awsConfig.buildClient();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<KinesisSourceP> processors = IntStream.range(0, count)
                .mapToObj(i -> new KinesisSourceP(kinesis, stream, hashRange.partition(i, count)))
                .collect(toList());
        return processors;
    }
}
