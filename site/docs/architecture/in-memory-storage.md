---
title: In-Memory Storage
---

Hazelcast Jet builds on a tight integration with Hazelcast IMDG – the
robust, distributed in-memory storage with querying and event-driven
programming support. The services of Hazelcast IMDG are available in the
Jet cluster to be used in conjunction with the Jet Pipelines.

## IMap

TODO

IMap is a distributed key-value store.

Use it  distriuted key-value store to cache the reference data and
enrich the event stream with it. See the [code
sample](https://github.com/hazelcast/hazelcast-jet/blob/master/examples/enrichment/src/main/java/com/hazelcast/jet/examples/enrichment/Enrichment.java)

Cache the results of the Jet computation. The cache can be queryied
using indexes and supports event-listeners to push updates to subscribed
clients. See the [code
sample](https://github.com/hazelcast/hazelcast-jet/tree/master/examples/imdg-connectors/src/main/java/com/hazelcast/jet/examples/imdg)

Load the input data from disk-based storages (S3, HDFS, files,
databases) to the cluster cache for faster processing with Jet. See the
[code
sample](https://github.com/hazelcast/big-data-benchmark/tree/master/word-count/hdfs-to-map)

## IMap Journal

TODO

## Coordination

TODO

Coordinate your application using a linear and distributed
implementation of the Java concurrency primitives backed by the Raft
consensus algorithm such as locks, atomics, semaphores, and latches. See
the [code
sample](https://github.com/hazelcast/hazelcast-code-samples/tree/master/cp-subsystem)