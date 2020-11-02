package com.hazelcast.jet.kinesis;

public class KinesisSinks {

    //todo, limitation on batch put records: Each PutRecords request can support
    // up to 500 records. Each record in the request can be as large as 1 MiB,
    // up to a limit of 5 MiB for the entire request, including partition keys.
    // Each shard can support writes up to 1,000 records per second, up to a
    // maximum data write total of 1 MiB per second.

}
