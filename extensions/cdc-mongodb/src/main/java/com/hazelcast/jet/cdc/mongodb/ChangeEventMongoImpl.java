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

import com.hazelcast.jet.cdc.ChangeEvent;
import com.hazelcast.jet.cdc.ChangeEventKey;
import com.hazelcast.jet.cdc.ChangeEventValue;

import javax.annotation.Nonnull;
import java.util.Objects;

public class ChangeEventMongoImpl implements ChangeEvent {

    private final String keyJson;
    private final String valueJson;

    private String json;
    private ChangeEventKey key;
    private ChangeEventValue value;

    public ChangeEventMongoImpl(@Nonnull String keyJson, @Nonnull String valueJson) {
        this.keyJson = Objects.requireNonNull(keyJson, "keyJson");
        this.valueJson = Objects.requireNonNull(valueJson, "valueJson");
    }

    @Override
    @Nonnull
    public ChangeEventKey key() {
        if (key == null) {
            key = new ChangeEventKeyMongoImpl(keyJson);
        }
        return key;
    }

    @Override
    @Nonnull
    public ChangeEventValue value() {
        if (value == null) {
            value = new ChangeEventValueMongoImpl(valueJson);
        }
        return value;
    }

    @Override
    @Nonnull
    public String asJson() {
        if (json == null) {
            json = String.format("key:{%s}, value:{%s}", keyJson, valueJson);
        }
        return json;
    }

    @Override
    public String toString() {
        return asJson();
    }

}
