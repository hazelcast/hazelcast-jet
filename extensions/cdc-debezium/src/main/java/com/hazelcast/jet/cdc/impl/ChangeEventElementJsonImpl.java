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

import com.fasterxml.jackson.jr.annotationsupport.JacksonAnnotationExtension;
import com.fasterxml.jackson.jr.ob.JSON;
import com.hazelcast.jet.cdc.ChangeEventElement;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

class ChangeEventElementJsonImpl implements ChangeEventElement, IdentifiedDataSerializable {

    private static final JSON J = JSON.builder().register(JacksonAnnotationExtension.std).build();

    private String json;
    private Map<String, Object> content;

    ChangeEventElementJsonImpl() { //needed for deserialization
    }

    ChangeEventElementJsonImpl(@Nonnull String json) {
        this.json = Objects.requireNonNull(json);
    }

    @Override
    @Nonnull
    public <T> T asPojo(Class<T> clazz) throws ParsingException {
        try {
            return J.beanFrom(clazz, json);
        } catch (IOException e) {
            throw new ParsingException(e.getMessage(), e);
        }
    }

    @Override
    public Map<String, Object> asMap() throws ParsingException {
        if (content == null) {
            try {
                content = J.mapFrom(json);
            } catch (IOException e) {
                throw new ParsingException(e.getMessage(), e);
            }
        }
        return content;
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
        out.writeUTF(json);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        json = in.readUTF();
    }
}
