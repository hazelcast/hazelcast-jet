package com.hazelcast.jet.pipeline;

/**
 * Interface for transforms that can be configured to use RocksDB state backend.
 */
public interface PersistableTransform {
    void setUsePersistence(boolean usePersistence);
}
