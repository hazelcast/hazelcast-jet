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

package com.hazelcast.jet.cdc.imp;

import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.jet.cdc.impl.SequenceExtractor;

import java.util.Map;
import java.util.Objects;

public class MySqlSequenceExtractor implements SequenceExtractor {

    private static final String SERVER = "server";
    private static final String BINLOG_FILE = "file";
    private static final String BINLOG_POSITION = "pos";

    private String server;
    private String binlog;
    private long partition;

    @Override
    public long sequence(Map<String, ?> debeziumOffset) {
        return (Long) debeziumOffset.get(BINLOG_POSITION);
    }

    @Override
    public long partition(Map<String, ?> debeziumPartition, Map<String, ?> debeziumOffset) {
        String server = (String) debeziumPartition.get(SERVER);
        String binlog = (String) debeziumOffset.get(BINLOG_FILE);
        if (isPartitionNew(server, binlog)) {
            long partition = computePartition(server, binlog);
            this.partition = adjustForCollision(partition);
            this.server = server;
            this.binlog = binlog;
        }
        return this.partition;
    }

    private boolean isPartitionNew(String server, String binlog) {
        return !Objects.equals(this.server, server) || !Objects.equals(this.binlog, binlog);
    }

    private long adjustForCollision(long partition) {
        if (this.partition == partition) {
            //partition value should have changed, but hashing unfortunately
            //produced the same result; we need to adjust it
            if (partition == Long.MAX_VALUE) {
                return Long.MIN_VALUE;
            } else {
                return Long.MAX_VALUE;
            }
        } else {
            return partition;
        }
    }

    private static long computePartition(String server, String binlog) {
        byte[] bytes = (server + binlog).getBytes();
        return HashUtil.MurmurHash3_x64_64(bytes, 0, bytes.length);
    }
}
