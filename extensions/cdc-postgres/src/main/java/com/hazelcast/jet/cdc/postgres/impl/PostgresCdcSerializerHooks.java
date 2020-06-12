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

import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class PostgresCdcSerializerHooks {

    public static final class PostgresChangeRecordImplHook implements SerializerHook<PostgresChangeRecordImpl> {

        @Override
        public Class<PostgresChangeRecordImpl> getSerializationType() {
            return PostgresChangeRecordImpl.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<PostgresChangeRecordImpl>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.CDC_RECORD;
                }

                @Override
                public void write(ObjectDataOutput out, PostgresChangeRecordImpl record) throws IOException {
                    out.writeLong(record.getSequenceSource());
                    out.writeLong(record.getSequenceValue());
                    out.writeUTF(record.getKeyJson());
                    out.writeUTF(record.getValueJson());
                }

                @Override
                public PostgresChangeRecordImpl read(ObjectDataInput in) throws IOException {
                    long sequenceSource = in.readLong();
                    long sequenceValue = in.readLong();
                    String keyJson = in.readUTF();
                    String valueJson = in.readUTF();
                    return new PostgresChangeRecordImpl(sequenceSource, sequenceValue, keyJson, valueJson);
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

}
