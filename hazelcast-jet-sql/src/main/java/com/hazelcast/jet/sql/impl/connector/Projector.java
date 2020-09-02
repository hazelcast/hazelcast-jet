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

import com.hazelcast.jet.sql.impl.inject.UpsertInjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTarget;
import com.hazelcast.sql.impl.type.QueryDataType;

import static com.hazelcast.jet.sql.impl.type.converter.ToConverters.getToConverter;

class Projector {

    private final UpsertTarget target;

    private final QueryDataType[] types;

    private final UpsertInjector[] injectors;

    Projector(
            UpsertTarget target,
            String[] paths,
            QueryDataType[] types
    ) {
        this.target = target;

        this.types = types;

        this.injectors = createInjectors(target, paths);
    }

    private static UpsertInjector[] createInjectors(
            UpsertTarget target,
            String[] paths
    ) {
        UpsertInjector[] injectors = new UpsertInjector[paths.length];
        for (int i = 0; i < paths.length; i++) {
            String path = paths[i];
            injectors[i] = target.createInjector(path);
        }
        return injectors;
    }

    Object project(Object[] row) {
        target.init();
        for (int i = 0; i < row.length; i++) {
            Object value = getToConverter(types[i]).convert(row[i]);
            injectors[i].set(value);
        }
        return target.conclude();
    }
}
