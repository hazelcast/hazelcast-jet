package com.hazelcast.jet.rocksdb;

import java.io.Serializable;

public class RocksDBOptionsBuilder implements Serializable {

    Integer memtableSize;
    Integer memtableNumber;
    Integer bloomFilterBits;
    Integer cacheSize;
    Integer subCompactions;

    /**
     * Sets RocksDB MemTable Size in bytes.
     */
    public RocksDBOptionsBuilder setMemtableSize(int memtableSize) {
        this.memtableSize = memtableSize;
        return this;
    }

    /**
     * Sets the number of RocksDB MemTables per ColumnFamily.
     */
    public RocksDBOptionsBuilder setMemtableNumber(int memtableNumber) {
        this.memtableNumber = memtableNumber;
        return this;
    }

    /**
     * Sets the number of bits used for RocksDB bloom filter.
     */
    public RocksDBOptionsBuilder setBloomFilterBits(int bloomFilterBits) {
        this.bloomFilterBits = bloomFilterBits;
        return this;
    }

    /**
     * Sets the size of RocksDB block cache in bytes.
     */
    public RocksDBOptionsBuilder setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    /**
     * Sets the number of threads to use for sub-compaction.
     */
    public RocksDBOptionsBuilder setSubCompactions(int subCompactions) {
        this.subCompactions = subCompactions;
        return this;
    }
}
