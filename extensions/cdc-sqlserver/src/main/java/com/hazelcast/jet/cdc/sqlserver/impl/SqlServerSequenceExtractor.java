/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cdc.sqlserver.impl;

import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.jet.cdc.impl.SequenceExtractor;
import io.debezium.connector.sqlserver.Lsn;

import java.util.Map;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SqlServerSequenceExtractor implements SequenceExtractor {

    private static final String SERVER = "server";
    private static final String CHANGE_LSN = "change_lsn";
    private static final String COMMIT_LSN = "commit_lsn";

    private static final long BYTE_MASK = 0xffL;

    private String server;
    private long source;

    @Override
    public long sequence(Map<String, ?> debeziumOffset) {
        String lsnString = (String) debeziumOffset.get(CHANGE_LSN);
        if (lsnString == null) {
            lsnString = (String) debeziumOffset.get(COMMIT_LSN);
        }

        long value = parseLsn(lsnString);
        System.err.println("\tvalue = " + value); //todo: remove

        return value;
    }

    private static long parseLsn(String lsnString) { //todo: lossy conversion, temporary solution only
        byte[] binary = Lsn.valueOf(lsnString).getBinary();
        long value = 0;
        for (int i = 0; i < Long.BYTES && i < binary.length; i++) {
            byte b = binary[binary.length - 1 - i];
            value += ((long) b & BYTE_MASK) << (Byte.SIZE * i);
        }
        return value;
    }

    @Override
    public long source(Map<String, ?> debeziumPartition, Map<String, ?> debeziumOffset) {
        String server = (String) debeziumPartition.get(SERVER);
        if (isSourceNew(server)) {
            long source = computeSource(server);
            this.source = adjustForCollision(source);
            this.server = server;
        }
        return this.source;
    }

    private boolean isSourceNew(String server) {
        return !Objects.equals(this.server, server);
    }

    private static long computeSource(String server) {
        byte[] bytes = server.getBytes(UTF_8);
        return HashUtil.MurmurHash3_x64_64(bytes, 0, bytes.length);
    }

    private long adjustForCollision(long source) {
        if (this.source == source) {
            //source value should have changed, but hashing unfortunately
            //produced the same result; we need to adjust it
            if (source == Long.MAX_VALUE) {
                return Long.MIN_VALUE;
            } else {
                return Long.MAX_VALUE;
            }
        } else {
            return source;
        }
    }
}
