package com.hazelcast.jet.rocksdb;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RocksDBRegistry {
    private static final ConcurrentMap<Long, RocksDBStateBackend> instances = new ConcurrentHashMap<>();

    public static RocksDBStateBackend getInstance(Long jobID) {
        return instances.computeIfAbsent(jobID, id -> new RocksDBStateBackend());
    }
}

