package com.hazelcast.jet.rocksdb;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.JetException;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.core.JetProperties.JET_HOME;

public class PrefixRocksDBStateBackend {

    private final AtomicInteger counter = new AtomicInteger(0);
    private final ArrayList<PrefixRocksMap> prefixMaps = new ArrayList<>();
    private PrefixRocksDBOptions prefixOptions = new PrefixRocksDBOptions();
    private volatile RocksDB db;
    private InternalSerializationService serializationService;
    private File directory;
    private Options dbOptions;

    /**
     * Sets user defined RocksDB options for PrefixRocksMap.
     */
    public void setPrefixOptions(PrefixRocksDBOptions prefixOptions) {
        this.prefixOptions = prefixOptions;
    }

    /**
     * Initialize the state backend with job-level serialization service and creates it directory.
     *
     * @param service   the serialization service configured for this job.
     * @param directory the directory where RocksDB creates its temp directory.
     */
    public PrefixRocksDBStateBackend initialize(InternalSerializationService service, String directory,
                                                long jobId) throws JetException {
        serializationService = service;
        this.directory = new File(directory + "/prefix-rocksdb-temp" + jobId);
        return this;
    }

    /**
     * Initialize the state backend with job-level serialization service and creates it directory.
     *
     * @param service the serialization service configured for this job.
     */
    public PrefixRocksDBStateBackend initialize(InternalSerializationService service, long jobId) throws JetException {
        initialize(service, defaultPath(), jobId);
        return this;
    }

    private String defaultPath() {
        String jetHome = new File(System.getProperty(JET_HOME.getName(), JET_HOME.getDefaultValue())).getAbsolutePath();
        return jetHome + "/rocksdb";
    }

    /**
     * Returns the directory where RocksDB instance will operate
     */
    public File directory() {
        return directory;
    }

    /**
     * Creates the associated RocksDB instance after the state backend is initialized.
     */
    public PrefixRocksDBStateBackend open() {
        if (db == null) {
            synchronized (this) {
                if (db == null) {
                    try {
                        if (!directory.mkdir()) {
                            throw new JetException("Failed to create RocksDB directory");
                        }
                        dbOptions = prefixOptions.options();
                        db = RocksDB.open(dbOptions, directory.toString());
                    } catch (RocksDBException e) {
                        throw new JetException("Failed to create a RocksDB instance", e);
                    }
                }
            }
        }
        return this;
    }


    /**
     * Returns a new PrefixRocksMap instance.
     *
     * @throws JetException if the database is closed.
     */
    @Nonnull
    public <K, V> PrefixRocksMap<K, V> getPrefixMap() throws JetException {
        assert db != null : "state backend was not opened";
        PrefixRocksMap<K, V> map = new PrefixRocksMap<>(db, getNextName(),
                new PrefixRocksDBOptions(prefixOptions), serializationService);
        prefixMaps.add(map);
        return map;
    }

    /**
     * Deletes the associated RocksDB instance.
     * Must be invoked when the job finishes execution.
     *
     * @throws JetException if the database is closed.
     */
    public void close() throws JetException {
        if (db != null) {
            for (PrefixRocksMap prefixRocksMap : prefixMaps) {
                prefixRocksMap.close();
            }
            dbOptions.close();
            db.close();
        }
    }

    @Nonnull
    private String getNextName() {
        return "PrefixRocksMap" + counter.getAndIncrement();
    }
}
