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
package com.hazelcast.jet.kinesis;

/**
 * JAVADOC
 */
public class ToDo {

    //todo: do not block in processor init

    //todo: make sure all integration tests run on real backend too

    //todo: merges and splits can reorder in-flight messages, document this
    //todo: offer option for single instance sources to fix reordering; test this

    //todo: test stream when shard count is more than 100 (manually, mock won't allow for it)

    //todo: saved shard offsets for recovery, maybe as shard id -- offset last seen map?
    // what happens after split/merge, should we delete the offset of CLOSED shards?

    //todo: clarify what license to use

    //todo: update source/sink docs

    //todo: use TestSupport to test the Kinesis processors

    //todo: delete this class

}
