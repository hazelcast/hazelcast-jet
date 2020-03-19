package com.hazelcast.jet.rocksdb;

/**
 * A factory class used to instantiate a RocksDBStateBackend.
 */
public class RocksDBFactory<K,V> {
    public RocksDBStateBackend<K,V> getKeyValueStore(){
        return new RocksDBStateBackend<K,V>(new RocksDBOptions().getOptions(),"path/to/db");
    }
}
