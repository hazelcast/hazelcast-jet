package com.hazelcast.jet.kinesis.impl;

import com.amazonaws.services.kinesis.model.Record;

import javax.annotation.Nonnull;
import java.util.List;

interface ShardWorker {

    @Nonnull
    List<Record> poll();

}
