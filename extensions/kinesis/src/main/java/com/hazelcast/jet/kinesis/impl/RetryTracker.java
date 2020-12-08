package com.hazelcast.jet.kinesis.impl;

import com.hazelcast.jet.retry.RetryStrategy;

public class RetryTracker {

    private final RetryStrategy strategy;

    private int attempt;

    public RetryTracker(RetryStrategy strategy) {
        this.strategy = strategy;
    }

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
