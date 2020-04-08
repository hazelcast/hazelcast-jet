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

import com.fasterxml.jackson.databind.JsonNode;
import com.hazelcast.jet.cdc.ChangeEventElement;
import com.hazelcast.jet.cdc.ChangeEventValue;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.impl.util.LazyThrowingSupplier;
import com.hazelcast.jet.cdc.impl.util.ThrowingSupplier;

import javax.annotation.Nonnull;
import java.util.Optional;

public class ChangeEventValueJsonImpl extends ChangeEventElementJsonImpl implements ChangeEventValue {

    private final String json;
    private final ThrowingSupplier<Optional<Long>, ParsingException> timestamp;
    private final ThrowingSupplier<Operation, ParsingException> operation;
    private final ThrowingSupplier<Optional<ChangeEventElement>, ParsingException> before;
    private final ThrowingSupplier<Optional<ChangeEventElement>, ParsingException> after;

    public ChangeEventValueJsonImpl(String valueJson) {
        super(valueJson);

        ThrowingSupplier<JsonNode, ParsingException> node = JsonParsing.parse(valueJson);
        this.timestamp = new LazyThrowingSupplier<>(() ->
                JsonParsing.getLong(node.get(), "ts_ms"));
        this.operation = new LazyThrowingSupplier<>(() ->
                Operation.get(JsonParsing.getString(node.get(), "op").orElse(null)));
        this.before = new LazyThrowingSupplier<>(() -> JsonParsing.getChild(node.get(), "before").get()
                .map(n -> new ChangeEventElementJsonImpl(n)));
        this.after = new LazyThrowingSupplier<>(() -> JsonParsing.getChild(node.get(), "after").get()
                .map(n -> new ChangeEventElementJsonImpl(n)));
        this.json = valueJson;
    }

    @Override
    public long timestamp() throws ParsingException {
        return timestamp.get().orElseThrow(() -> new ParsingException("No parsable timestamp field found"));
    }

    @Override
    @Nonnull
    public Operation operation() throws ParsingException {
        return operation.get();
    }

    @Override
    @Nonnull
    public ChangeEventElement before() throws ParsingException {
        return before.get().orElseThrow(() -> new ParsingException("No 'before' sub-node present"));
    }

    @Override
    @Nonnull
    public ChangeEventElement after() throws ParsingException {
        return after.get().orElseThrow(() -> new ParsingException("No 'after' sub-node present"));
    }

    @Override
    @Nonnull
    public ChangeEventElement change() {
        throw new UnsupportedOperationException("Not supported for relational databases");
    }

    @Override
    @Nonnull
    public String asJson() {
        return json;
    }

    @Override
    public String toString() {
        return asJson();
    }
}
