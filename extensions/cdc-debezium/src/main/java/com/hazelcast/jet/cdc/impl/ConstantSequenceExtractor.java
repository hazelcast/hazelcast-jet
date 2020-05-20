package com.hazelcast.jet.cdc.impl;

import java.util.Map;

public class ConstantSequenceExtractor implements SequenceExtractor {

    @Override
    public long partition(Map<String, ?> debeziumPartition, Map<String, ?> debeziumOffset) {
        return 0;
    }

    @Override
    public long sequence(Map<String, ?> debeziumOffset) {
        return 0;
    }
}
