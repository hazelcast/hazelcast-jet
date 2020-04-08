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
import com.hazelcast.jet.cdc.ChangeEventElement;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.impl.util.LazyThrowingSupplier;
import com.hazelcast.jet.cdc.impl.util.ThrowingBiFunction;
import com.hazelcast.jet.cdc.impl.util.ThrowingFunction;
import com.hazelcast.jet.cdc.impl.util.ThrowingSupplier;
import org.bson.Document;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

class ChangeEventElementMongoImpl implements ChangeEventElement {

    private final ThrowingSupplier<Document, ParsingException> document;
    private final SupplierEx<String> json;

    private final ThrowingFunction<String, Optional<Object>, ParsingException> objects;
    private final ThrowingFunction<String, Optional<String>, ParsingException> strings;
    private final ThrowingFunction<String, Optional<Integer>, ParsingException> integers;
    private final ThrowingFunction<String, Optional<Long>, ParsingException> longs;
    private final ThrowingFunction<String, Optional<Double>, ParsingException> doubles;
    private final ThrowingFunction<String, Optional<Boolean>, ParsingException> booleans;
    private final ThrowingBiFunction<String, Class<Object>, Optional<List<Optional<Object>>>, ParsingException> lists;

    ChangeEventElementMongoImpl(String json) {
        this(new LazyThrowingSupplier<>(MongoParsing.parse(json)), () -> json);
    }

    ChangeEventElementMongoImpl(Document document) {
        this(() -> document, document::toJson);
    }

    private ChangeEventElementMongoImpl(ThrowingSupplier<Document, ParsingException> document, SupplierEx<String> json) {
        this.document = document;
        this.json = json;

        this.objects = (key) -> MongoParsing.getObject(document.get(), key);
        this.strings = (key) -> MongoParsing.getString(document.get(), key);
        this.integers = (key) -> MongoParsing.getInteger(document.get(), key);
        this.longs = (key) -> MongoParsing.getLong(document.get(), key);
        this.doubles = (key) -> MongoParsing.getDouble(document.get(), key);
        this.booleans = (key) -> MongoParsing.getBoolean(document.get(), key);
        this.lists = (key, clazz) -> MongoParsing.getList(document.get(), key, clazz);
    }

    @Override
    @Nonnull
    public <T> T mapToObj(Class<T> clazz) throws ParsingException {
        if (!clazz.equals(Document.class)) {
            throw new IllegalArgumentException("Content provided only as " + Document.class.getName());
        }
        return (T) document.get();
    }

    @Override
    @Nonnull
    public Optional<Object> getObject(String key) throws ParsingException {
        return objects.apply(key);
    }

    @Override
    @Nonnull
    public Optional<String> getString(String key) throws ParsingException {
        return strings.apply(key);
    }

    @Override
    @Nonnull
    public Optional<Integer> getInteger(String key) throws ParsingException {
        return integers.apply(key);
    }

    @Override
    @Nonnull
    public Optional<Long> getLong(String key) throws ParsingException {
        return longs.apply(key);
    }

    @Override
    @Nonnull
    public Optional<Double> getDouble(String key) throws ParsingException {
        return doubles.apply(key);
    }

    @Override
    @Nonnull
    public Optional<Boolean> getBoolean(String key) throws ParsingException {
        return booleans.apply(key);
    }

    @Override
    @Nonnull
    public <T> Optional<List<Optional<T>>> getList(String key, Class<T> clazz) throws ParsingException {
        return lists.apply(key, (Class) clazz);
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
