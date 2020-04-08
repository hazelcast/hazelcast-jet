/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cdc.mongodb;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.cdc.ChangeEvent;
import com.hazelcast.jet.cdc.ChangeEventKey;
import com.hazelcast.jet.cdc.ChangeEventValue;
import com.hazelcast.jet.cdc.impl.util.LazySupplier;

import javax.annotation.Nonnull;

public class ChangeEventMongoImpl implements ChangeEvent {

    private final SupplierEx<String> json;
    private final SupplierEx<ChangeEventKey> key;
    private final SupplierEx<ChangeEventValue> value;

    public ChangeEventMongoImpl(@Nonnull String keyJson, @Nonnull String valueJson) {
        this.key = new LazySupplier<>(() -> new ChangeEventKeyMongoImpl(keyJson));
        this.value = new LazySupplier<>(() -> new ChangeEventValueMongoImpl(valueJson));
        this.json = new LazySupplier<>(() -> String.format("key:{%s}, value:{%s}", keyJson, valueJson));
    }

    @Override
    @Nonnull
    public ChangeEventKey key() {
        return key.get();
    }

    @Override
    @Nonnull
    public ChangeEventValue value() {
        return value.get();
    }

    @Override
    @Nonnull
    public String asJson() {
        return json.get();
    }

    @Override
    public String toString() {
        return asJson();
    }

}
