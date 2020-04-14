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

package com.hazelcast.jet.cdc.impl;

import com.hazelcast.jet.cdc.ChangeEventElement;
import com.hazelcast.jet.cdc.ChangeEventValue;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.ParsingException;

import javax.annotation.Nonnull;

public class ChangeEventValueJsonImpl extends ChangeEventElementJsonImpl implements ChangeEventValue {

    private Long timestamp;
    private Operation operation;
    private ChangeEventElement before;
    private ChangeEventElement after;

    ChangeEventValueJsonImpl() { //needed for deserialization
    }

    public ChangeEventValueJsonImpl(String valueJson) {
        super(valueJson);
    }

    @Override
    public long timestamp() throws ParsingException {
        if (timestamp == null) {
            timestamp = JsonParsing.getLong(node(), "ts_ms")
                    .orElseThrow(() -> new ParsingException("No parsable timestamp field found"));
        }
        return timestamp;
    }

    @Override
    @Nonnull
    public Operation operation() throws ParsingException {
        if (operation == null) {
            operation = Operation.get(JsonParsing.getString(node(), "op").orElse(null));
        }
        return operation;
    }

    @Override
    @Nonnull
    public ChangeEventElement before() throws ParsingException {
        if (before == null) {
            before = JsonParsing.getChild(node(), "before")
                    .map(ChangeEventElementJsonImpl::new)
                    .orElseThrow(() -> new ParsingException("No 'before' sub-node present"));
        }
        return before;
    }

    @Override
    @Nonnull
    public ChangeEventElement after() throws ParsingException {
        if (after == null) {
            after = JsonParsing.getChild(node(), "after")
                    .map(ChangeEventElementJsonImpl::new)
                    .orElseThrow(() -> new ParsingException("No 'after' sub-node present"));
        }
        return after;
    }

    @Override
    public int getClassId() {
        return CdcJsonDataSerializerHook.VALUE;
    }
}
