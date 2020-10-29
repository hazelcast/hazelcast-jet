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

    //todo: extract aws config params into a wrapping class
    //todo: make params serializable

    //todo: Use ranges only for initial shard distribution, handle splits and merges internally;
    // splits: processor handles all children, merge: processor that handled parent handles it,
    // decide based on two parents

    //todo: test source when stream doesn't exist
    //todo: test stream when shard count is more than 100

    //todo: share container for tests, just use different streams in each test

    //todo: saved shard offsets for recovery, maybe as shard id -- offset last seen map?
    // what happens after split/merge, should we delete the offset of CLOSED shards?

    //todo: clarify what license to use
    //todo: handle Kinesis quotas: https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
    //todo: delete this class

    //todo: test that does random shard splits & merges mixed with data updates and checks that all data is processed

    //todo: @since tags
    //todo: @Nonnull/@Nullable annotations
    //todo: javadoc

    //is one AmazonKinesisAsync per Jet member enough? how does performance look like?

    //todo: update source/sink docs
    //todo: tutorial
    //todo: deployment guide?


    /*
{ShardId: shardId-000000000000,HashKeyRange: {StartingHashKey: 0,EndingHashKey: 340282366920938463463374607431768211455},SequenceNumberRange: {StartingSequenceNumber: 49612142550585946531422261160855796901439256263990968322,}}

{ShardId: shardId-000000000000,HashKeyRange: {StartingHashKey: 0,EndingHashKey: 170141183460469231731687303715884105727},SequenceNumberRange: {StartingSequenceNumber: 49612142605534982700601716581599806725244829597526130690,}}
{ShardId: shardId-000000000001,HashKeyRange: {StartingHashKey: 170141183460469231731687303715884105728,EndingHashKey: 340282366920938463463374607431768211455},SequenceNumberRange: {StartingSequenceNumber: 49612142605557283445800247204741342443517477959032111122,}}

{ShardId: shardId-000000000000,HashKeyRange: {StartingHashKey: 0,EndingHashKey: 113427455640312821154458202477256070484},SequenceNumberRange: {StartingSequenceNumber: 49612142670474752718722891169751818335196870809885868034,}}
{ShardId: shardId-000000000001,HashKeyRange: {StartingHashKey: 113427455640312821154458202477256070485,EndingHashKey: 226854911280625642308916404954512140969},SequenceNumberRange: {StartingSequenceNumber: 49612142670497053463921421792893354053469519171391848466,}}
{ShardId: shardId-000000000002,HashKeyRange: {StartingHashKey: 226854911280625642308916404954512140970,EndingHashKey: 340282366920938463463374607431768211455},SequenceNumberRange: {StartingSequenceNumber: 49612142670519354209119952416034889771742167532897828898,}}
     */
}
