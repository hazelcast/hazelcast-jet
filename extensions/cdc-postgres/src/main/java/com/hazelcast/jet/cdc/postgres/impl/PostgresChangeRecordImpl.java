/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    @Override
    public int hashCode() {
        return super.hashCode(); //extra field is recomputed on the fly
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return super.equals(obj);
    }
}
