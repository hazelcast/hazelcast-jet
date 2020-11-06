package com.hazelcast.jet.kinesis.impl;

import com.amazonaws.services.kinesis.model.Shard;

final class KinesisUtil {

    static boolean shardBelongsToRange(Shard shard, HashRange range) {
        String startingHashKey = shard.getHashKeyRange().getStartingHashKey();
        return range.contains(startingHashKey);
    }
}
