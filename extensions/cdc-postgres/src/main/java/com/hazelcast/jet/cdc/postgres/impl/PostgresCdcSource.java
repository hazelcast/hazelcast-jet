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
