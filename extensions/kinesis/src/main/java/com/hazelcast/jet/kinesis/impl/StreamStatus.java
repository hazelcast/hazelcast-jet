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
package com.hazelcast.jet.kinesis.impl;

enum StreamStatus {
    CREATING("CREATING"),   // The stream is being created.
    DELETING("DELETING"),   // The stream is being deleted.
    ACTIVE("ACTIVE"),       // The stream exists and is ready for read and write operations or deletion.
    UPDATING("UPDATING")    // Shards in the stream are being merged or split.
    ;

    private final String status;

    StreamStatus(String status) {
        this.status = status;
    }

    public boolean is(String testedStatus) {
        return status.equals(testedStatus);
    }
}
