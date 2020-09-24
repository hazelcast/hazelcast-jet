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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public final class CsvQueryTargetDescriptor implements QueryTargetDescriptor {

    private Map<String, Integer> indicesByNames;

    @SuppressWarnings("unused")
    private CsvQueryTargetDescriptor() {
    }

    public CsvQueryTargetDescriptor(Map<String, Integer> indicesByNames) {
        this.indicesByNames = indicesByNames;
    }

    @Override
    public QueryTarget create(InternalSerializationService serializationService, Extractors extractors, boolean isKey) {
        return new CsvQueryTarget(indicesByNames);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(indicesByNames);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        indicesByNames = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CsvQueryTargetDescriptor that = (CsvQueryTargetDescriptor) o;
        return Objects.equals(indicesByNames, that.indicesByNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indicesByNames);
    }
}
