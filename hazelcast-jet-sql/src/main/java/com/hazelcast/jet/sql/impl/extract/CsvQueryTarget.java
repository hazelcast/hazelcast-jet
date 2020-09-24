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

package com.hazelcast.jet.sql.impl.extract;

import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Map;

@NotThreadSafe
public class CsvQueryTarget implements QueryTarget {

    private final Map<String, Integer> indicesByNames;

    private String[] line;

    public CsvQueryTarget(Map<String, Integer> indicesByNames) {
        this.indicesByNames = indicesByNames;
    }

    @Override
    public void setTarget(Object target) {
        line = (String[]) target;
    }

    @Override
    public QueryExtractor createExtractor(String name, QueryDataType type) {
        Integer index = indicesByNames.get(name);
        if (index == null) {
            return () -> null;
        } else {
            return () -> type.convert(line[index]);
        }
    }
}
