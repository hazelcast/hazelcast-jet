/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.observer;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

public class ObservableBatch implements IdentifiedDataSerializable {

    private static final ObservableBatch END_OF_DATA = new ObservableBatch(null, null);
    @Nullable
    private Object[] items;
    @Nullable
    private Throwable throwable;

    private ObservableBatch(@Nullable Object[] items, @Nullable Throwable throwable) {
        this.items = items;
        this.throwable = throwable;
    }

    ObservableBatch() { //needed for deserialization
    }

    public static ObservableBatch items(@Nonnull ArrayList<Object> items) {
        Objects.requireNonNull(items, "items");
        return new ObservableBatch(items.toArray(), null);
    }

    static ObservableBatch endOfData() {
        return END_OF_DATA;
    }

    public static ObservableBatch error(Throwable throwable) {
        Objects.requireNonNull(throwable, "throwable");
        return new ObservableBatch(null, throwable);
    }

    @Nullable
    Object[] getItems() {
        return items;
    }

    @Nullable
    Throwable getThrowable() {
        return throwable;
    }

    @Override
    public int getFactoryId() {
        return JetObserverDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetObserverDataSerializerHook.OBSERVABLE_BATCH;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(items);
        out.writeObject(throwable);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        items = in.readObject();
        throwable = in.readObject();
    }
}
