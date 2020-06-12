package com.hazelcast.jet.cdc.postgres.impl;

import com.hazelcast.jet.cdc.impl.CdcSource;
import com.hazelcast.jet.cdc.impl.ChangeRecordImpl;

import java.util.Properties;

public class PostgresCdcSource extends CdcSource {

    public PostgresCdcSource(Properties properties) {
        super(properties);
    }

    @Override
    protected ChangeRecordImpl changeRecord(long sequenceSource, long sequenceValue, String keyJson, String valueJson) {
        return new PostgresChangeRecordImpl(sequenceSource, sequenceValue, keyJson, valueJson);
    }

}
