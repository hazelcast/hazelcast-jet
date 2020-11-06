package com.hazelcast.jet.kinesis.impl;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;

abstract class AbstractShardWorker { //todo: makes sense?

    protected final AmazonKinesisAsync kinesis;
    protected final String stream;

    AbstractShardWorker(AmazonKinesisAsync kinesis, String stream) {
        this.kinesis = kinesis;
        this.stream = stream;
    }
}
