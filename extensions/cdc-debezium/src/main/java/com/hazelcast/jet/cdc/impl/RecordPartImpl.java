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

import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.RecordPart;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

class RecordPartImpl implements RecordPart {

    private String json;
    private Map<String, Object> content;

    RecordPartImpl(@Nonnull String json) {
        this.json = Objects.requireNonNull(json);
    }

    @Override
    @Nonnull
    public <T> T toObject(@Nonnull Class<T> clazz) throws ParsingException {
        Objects.requireNonNull(clazz, "class");
        try {
            return JsonUtil.parse(clazz, json);
        } catch (Throwable e) {
            throw new ParsingException(e.getMessage(), e);
        }
    }

    @Override
    @Nonnull
    public Map<String, Object> toMap() throws ParsingException {
        if (content == null) {
            try {
                content = JsonUtil.parse(json);
            } catch (Throwable e) {
                throw new ParsingException(e.getMessage(), e);
            }
        }
        return content;
    }

    @Override
    @Nonnull
    public String toJson() {
        return json;
    }

    @Override
    public String toString() {
        return toJson();
    }

    void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(json);
    }

    static RecordPartImpl readData(ObjectDataInput in) throws IOException {
        String json = in.readUTF();
        return new RecordPartImpl(json);
    }

}
