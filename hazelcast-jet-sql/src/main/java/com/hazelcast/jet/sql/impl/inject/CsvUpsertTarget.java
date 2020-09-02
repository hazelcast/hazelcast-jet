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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.sql.impl.type.QueryDataType;

// TODO: can it be non-thread safe ?
class CsvUpsertTarget implements UpsertTarget {

    private final StringBuilder line;
    private final String delimiter;

    private int i;

    CsvUpsertTarget(String delimiter) {
        this.line = new StringBuilder();
        this.delimiter = delimiter;
    }

    @Override
    public UpsertInjector createInjector(String path) {
        return value -> {
            if (i++ > 0) {
                line.append(delimiter);
            }

            line.append(QueryDataType.VARCHAR.convert(value));
        };
    }

    @Override
    public void init() {
        line.setLength(0);
        i = 0;
    }

    @Override
    public Object conclude() {
        return line.toString();
    }
}
