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
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

class ChangeEventElementJsonImpl implements ChangeEventElement, IdentifiedDataSerializable {

    private String json;
    private JsonNode node;

    ChangeEventElementJsonImpl() { //needed for deserialization
    }

    ChangeEventElementJsonImpl(@Nonnull String json) {
        this(null, json);
    }

    ChangeEventElementJsonImpl(@Nonnull JsonNode node) {
        this(node, null);
    }

    private ChangeEventElementJsonImpl(
            @Nullable JsonNode node,
            @Nullable String json) {
        if (node == null && json == null) {
            throw new IllegalStateException("Need some input");
        }
        this.json = json;
        this.node = node;
    }

    @Override
    @Nonnull
    public <T> T mapToObj(Class<T> clazz) throws ParsingException {
        return JsonParsing.mapToObj(node(), clazz);
    }

    @Override
    @Nonnull
    public Optional<ChangeEventElement> getChild(String key) throws ParsingException {
        return JsonParsing.getChild(node(), key).map(ChangeEventElementJsonImpl::new);
    }

    @Override
    @Nonnull
    public Optional<Object> getRaw(String key) throws ParsingException {
        return JsonParsing.getObject(node(), key);
    }

    @Override
    @Nonnull
    public Optional<String> getString(String key) throws ParsingException {
        return JsonParsing.getString(node(), key);
    }

    @Override
    @Nonnull
    public Optional<Integer> getInteger(String key) throws ParsingException {
        return JsonParsing.getInteger(node(), key);
    }

    @Override
    @Nonnull
    public Optional<Long> getLong(String key) throws ParsingException {
        return JsonParsing.getLong(node(), key);
    }

    @Override
    @Nonnull
    public Optional<Double> getDouble(String key) throws ParsingException {
        return JsonParsing.getDouble(node(), key);
    }

    @Override
    @Nonnull
    public Optional<Boolean> getBoolean(String key) throws ParsingException {
        return JsonParsing.getBoolean(node(), key);
    }

    @Override
    @Nonnull
    public <T> Optional<List<Optional<T>>> getList(String key, Class<T> clazz) throws ParsingException {
        return JsonParsing.getList(node(), key, clazz);
    }

    protected JsonNode node() throws ParsingException {
        if (node == null) {
            node = JsonParsing.parse(json);
        }
        return node;
    }

    @Override
    @Nonnull
    public String asJson() {
        if (json == null) {
            json = node.textValue();
        }
        return json;
    }

    @Override
    public String toString() {
        return asJson();
    }

    @Override
    public int getFactoryId() {
        return CdcJsonDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CdcJsonDataSerializerHook.ELEMENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(asJson());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        json = in.readUTF();
    }
}
