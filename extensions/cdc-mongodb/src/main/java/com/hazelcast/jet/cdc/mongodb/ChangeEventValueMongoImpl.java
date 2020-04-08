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

import com.hazelcast.jet.cdc.ChangeEventElement;
import com.hazelcast.jet.cdc.ChangeEventValue;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.impl.util.LazyThrowingSupplier;
import com.hazelcast.jet.cdc.impl.util.ThrowingSupplier;
import org.bson.Document;

import javax.annotation.Nonnull;
import java.util.Optional;

import static com.hazelcast.jet.cdc.mongodb.MongoParsing.getDocument;
import static com.hazelcast.jet.cdc.mongodb.MongoParsing.parse;

public class ChangeEventValueMongoImpl extends ChangeEventElementMongoImpl implements ChangeEventValue {

    private final String json;
    private final ThrowingSupplier<Optional<Long>, ParsingException> timestamp;
    private final ThrowingSupplier<Operation, ParsingException> operation;
    private final ThrowingSupplier<Optional<ChangeEventElement>, ParsingException> before;
    private final ThrowingSupplier<Optional<ChangeEventElement>, ParsingException> after;
    private final ThrowingSupplier<Optional<ChangeEventElement>, ParsingException> patch;

    public ChangeEventValueMongoImpl(String valueJson) {
        super(valueJson);

        ThrowingSupplier<Document, ParsingException> document = parse(valueJson);
        this.timestamp = new LazyThrowingSupplier<>(() ->
                MongoParsing.getLong(document.get(), "ts_ms"));
        this.operation = new LazyThrowingSupplier<>(() ->
                Operation.get(MongoParsing.getString(document.get(), "op").orElse(null)));
        this.before = new LazyThrowingSupplier<>(() ->
                getDocument(document.get(), "before").get().map(ChangeEventElementMongoImpl::new));
        this.after = new LazyThrowingSupplier<>(() ->
                getDocument(document.get(), "after").get().map(ChangeEventElementMongoImpl::new));
        this.patch = new LazyThrowingSupplier<>(() ->
                getDocument(document.get(), "patch").get().map(ChangeEventElementMongoImpl::new));
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
        return before.get().orElseThrow(() -> new ParsingException("No 'before' sub-document present"));
    }

    @Override
    @Nonnull
    public ChangeEventElement after() throws ParsingException {
        return after.get().orElseThrow(() -> new ParsingException("No 'after' sub-document present"));
    }

    @Override
    @Nonnull
    public ChangeEventElement change() throws ParsingException {
        return patch.get().orElseThrow(() -> new ParsingException("No 'patch' sub-document present"));
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
