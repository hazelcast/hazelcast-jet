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

import javax.annotation.Nonnull;

import static com.hazelcast.jet.cdc.mongodb.MongoParsing.getChild;

public class ChangeEventValueMongoImpl extends ChangeEventElementMongoImpl implements ChangeEventValue {

    private Long timestamp;
    private Operation operation;
    private ChangeEventElement before;
    private ChangeEventElement after;
    private ChangeEventElement patch;

    public ChangeEventValueMongoImpl(String valueJson) {
        super(valueJson);
    }

    @Override
    public long timestamp() throws ParsingException {
        if (timestamp == null) {
            timestamp = MongoParsing.getLong(document(), "ts_ms")
                    .orElseThrow(() -> new ParsingException("No parsable timestamp field found"));
        }
        return timestamp;
    }

    @Override
    @Nonnull
    public Operation operation() throws ParsingException {
        if (operation == null) {
            operation = Operation.get(MongoParsing.getString(document(), "op").orElse(null));
        }
        return operation;
    }

    @Override
    @Nonnull
    public ChangeEventElement before() throws ParsingException {
        if (before == null) {
            before = getChild(document(), "before")
                    .map(ChangeEventElementMongoImpl::new)
                    .orElseThrow(() -> new ParsingException("No 'before' sub-document present"));
        }
        return before;
    }

    @Override
    @Nonnull
    public ChangeEventElement after() throws ParsingException {
        if (after == null) {
            after = getChild(document(), "after")
                    .map(ChangeEventElementMongoImpl::new)
                    .orElseThrow(() -> new ParsingException("No 'after' sub-document present"));
        }
        return after;
    }

    @Override
    @Nonnull
    public ChangeEventElement change() throws ParsingException {
        if (patch == null) {
            patch = getChild(document(), "patch")
                    .map(ChangeEventElementMongoImpl::new)
                    .orElseThrow(() -> new ParsingException("No 'patch' sub-document present"));
        }
        return patch;
    }

}
