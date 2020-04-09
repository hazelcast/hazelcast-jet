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
import com.hazelcast.jet.cdc.ParsingException;
import org.bson.Document;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

class ChangeEventElementMongoImpl implements ChangeEventElement {

    private Document document;
    private String json;

    ChangeEventElementMongoImpl(String json) {
        this(null, json);
    }

    ChangeEventElementMongoImpl(Document document) {
        this(document, null);
    }

    private ChangeEventElementMongoImpl(@Nullable Document document, @Nullable String json) {
        if (document == null && json == null) {
            throw new IllegalStateException("Need some input");
        }
        this.document = document;
        this.json = json;
    }

    @Override
    @Nonnull
    public <T> T mapToObj(Class<T> clazz) throws ParsingException {
        if (!clazz.equals(Document.class)) {
            throw new IllegalArgumentException("Content provided only as " + Document.class.getName());
        }
        return (T) document();
    }

    @Override
    @Nonnull
    public Optional<Object> getObject(String key) throws ParsingException {
        return MongoParsing.getObject(document(), key);
    }

    @Override
    @Nonnull
    public Optional<String> getString(String key) throws ParsingException {
        return MongoParsing.getString(document(), key);
    }

    @Override
    @Nonnull
    public Optional<Integer> getInteger(String key) throws ParsingException {
        return MongoParsing.getInteger(document(), key);
    }

    @Override
    @Nonnull
    public Optional<Long> getLong(String key) throws ParsingException {
        return MongoParsing.getLong(document(), key);
    }

    @Override
    @Nonnull
    public Optional<Double> getDouble(String key) throws ParsingException {
        return MongoParsing.getDouble(document(), key);
    }

    @Override
    @Nonnull
    public Optional<Boolean> getBoolean(String key) throws ParsingException {
        return MongoParsing.getBoolean(document(), key);
    }

    @Override
    @Nonnull
    public <T> Optional<List<Optional<T>>> getList(String key, Class<T> clazz) throws ParsingException {
        return MongoParsing.getList(document(), key, clazz);
    }

    protected Document document() throws ParsingException {
        if (document == null) {
            document = MongoParsing.parse(json);
        }
        return document;
    }

    @Override
    @Nonnull
    public String asJson() {
        if (json == null) {
            json = document.toJson();
        }
        return json;
    }

    @Override
    public String toString() {
        return asJson();
    }


}
