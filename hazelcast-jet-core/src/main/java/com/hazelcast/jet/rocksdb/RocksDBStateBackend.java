package com.hazelcast.jet.rocksdb;

import org.rocksdb.*;

import java.util.ArrayList;

/** Responsible for managing one RocksDB instance,
 *	opening and Closing the connection and deleting the database.
 *	Processors acquire an instance of this class on initialization.
 *	Processors use this class to acquire any number of RocksMaps they require.
 *	The datastore is logically partitioned using column families.
 *	Each RocksMap is instantiated with a ColumnFamilyHandler.
 *	Once the task has finished (complete() is invoked),
 *  the task asks it to delete the whole data-store.
 */

public class RocksDBStateBackend<K,V> {
    private RocksDB db;
    private ArrayList<ColumnFamilyHandle> cfhs= new ArrayList<>();

    public RocksDBStateBackend(Options opt,String directory) {
        RocksDB.loadLibrary();
        try {
            db = RocksDB.open(opt,directory);
        }
        catch(RocksDBException e){}
    }
    public RocksMap<K,V> getMap(){
        ColumnFamilyHandle cfh = null;
        try {
            cfh = db.createColumnFamily(new ColumnFamilyDescriptor("RocksMap1".getBytes()));
        } catch (RocksDBException e) {}
        cfhs.add(cfh);
        return new RocksMap<K,V>(db,cfh);
    }

    public void deleteDataStore(){}
}
