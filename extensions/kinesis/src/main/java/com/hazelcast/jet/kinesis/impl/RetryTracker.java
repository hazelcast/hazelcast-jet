package com.hazelcast.jet.kinesis.impl;

import com.hazelcast.jet.retry.RetryStrategy;

public class RetryTracker {

    private final RetryStrategy strategy = KinesisHelper.RETRY_STRATEGY;

    private int attempt;

    public void reset() {
        this.attempt = 0;
    }

    public void attemptFailed() {
        attempt++;
    }

    public long getNextWaitTimeMillis() {
        return strategy.getIntervalFunction().waitAfterAttempt(attempt);
    }
}
