package com.hazelcast.jet.rocksdb;

import org.rocksdb.Options;
/**
 * A configuration class where the RocksDB default options should be placed.
 * Used by RocksDBFactory to Create RocksDBStateBackend.
 * Note : RocksDB.Options extends DBOptions and ColumnFamilyOptions
 * it can be used to configure database-wide and column family options
 */
public class RocksDBOptions {
    public Options getOptions() {
        return null;
    }
}
