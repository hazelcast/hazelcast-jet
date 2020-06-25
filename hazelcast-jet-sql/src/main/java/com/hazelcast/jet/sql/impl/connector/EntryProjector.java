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

package com.hazelcast.jet.sql.impl.connector;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.sql.impl.inject.UpsertInjector;
import com.hazelcast.sql.impl.inject.UpsertTarget;
import com.hazelcast.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.sql.impl.type.converter.ToConverters.getToConverter;

class EntryProjector {

    private final UpsertTarget keyTarget;
    private final UpsertTarget valueTarget;

    private final QueryDataType[] types;

    private final UpsertInjector[] injectors;

    EntryProjector(
            UpsertTargetDescriptor keyDescriptor,
            UpsertTargetDescriptor valueDescriptor,
            InternalSerializationService serializationService,
            String[] names,
            QueryDataType[] types,
            boolean[] keys
    ) {
        this.keyTarget = keyDescriptor.create(serializationService);
        this.valueTarget = valueDescriptor.create(serializationService);

        this.types = types;

        this.injectors = createInjectors(keyTarget, valueTarget, names, keys);
    }

    private static UpsertInjector[] createInjectors(
            UpsertTarget keyTarget,
            UpsertTarget valueTarget,
            String[] names,
            boolean[] keys
    ) {
        UpsertInjector[] injectors = new UpsertInjector[names.length];
        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            injectors[i] = keys[i] ? keyTarget.createInjector(name) : valueTarget.createInjector(name);
        }
        return injectors;
    }

    Entry<Object, Object> project(Object[] row) {
        keyTarget.init();
        valueTarget.init();
        for (int i = 0; i < row.length; i++) {
            Object value = getToConverter(types[i]).convert(row[i]);
            injectors[i].set(value);
        }
        return entry(keyTarget.conclude(), valueTarget.conclude());
    }
}
