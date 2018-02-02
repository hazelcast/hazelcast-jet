/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream.impl.distributed;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

import static com.hazelcast.jet.stream.impl.StreamUtil.setPrivateField;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class DistributedIntSummaryStatistics extends java.util.IntSummaryStatistics implements DataSerializable {

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(this.getCount());
        out.writeLong(this.getSum());
        out.writeInt(this.getMin());
        out.writeInt(this.getMax());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        try {
            Class<?> clazz = java.util.IntSummaryStatistics.class;
            setPrivateField(this, clazz, "count", in.readLong());
            setPrivateField(this, clazz, "sum", in.readLong());
            setPrivateField(this, clazz, "min", in.readInt());
            setPrivateField(this, clazz, "max", in.readInt());
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw rethrow(e);
        }
    }
}
