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

    //todo: tripping any shard's ingestion rate, getting messages refused randomly from the send batch will mess
    // up the order of messages; document this

    //todo: merges and splits can reorder in-flight messages, document this
    //todo: offer option for single instance sources to fix reordering; test this

    //todo: clarify what license to use

    //todo: update source/sink docs

    //todo: use TestSupport to test the Kinesis processors

    //todo: test the sink and source in a real cluster with multiple nodes running in different JVMs

    //todo: delete this class

}
