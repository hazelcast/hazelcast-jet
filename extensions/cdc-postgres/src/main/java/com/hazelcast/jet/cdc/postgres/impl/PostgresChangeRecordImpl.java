package com.hazelcast.jet.cdc.postgres.impl;

import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.impl.ChangeRecordImpl;

import javax.annotation.Nonnull;

public class PostgresChangeRecordImpl extends ChangeRecordImpl {

    private String databaseAndSchema;

    public PostgresChangeRecordImpl(
            long sequenceSource,
            long sequenceValue,
            @Nonnull String keyJson,
            @Nonnull String valueJson
    ) {
        super(sequenceSource, sequenceValue, keyJson, valueJson);
    }

    @Nonnull
    @Override
    public String database() throws ParsingException {
        if (databaseAndSchema == null) {
            String schema = get(value().toMap(), "__schema", String.class);
            if (schema == null) {
                throw new ParsingException("No parsable schema name field found");
            }
            databaseAndSchema = super.database() + "." + schema;
        }
        return databaseAndSchema;
    }
}
