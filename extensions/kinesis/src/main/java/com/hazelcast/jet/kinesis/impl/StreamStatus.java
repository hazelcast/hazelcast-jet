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
