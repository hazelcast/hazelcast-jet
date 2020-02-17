---
title: Introduction
id: intro
---

Welcome to Hazelcast Jet!

Hazelcast Jet allows you to write modern, lambda-oriented Java code
that focuses purely on data transformation while it does all the heavy
lifting of getting the data flowing and computation running on all the
hardware you allocate to it. The flows can be both bounded (batch jobs)
and unbounded (streaming jobs).

These are some of the concerns Jet handles well:

- parallelizing across all CPU cores
- distributing across all cluster nodes
- load-balancing
- auto-scaling to newly added nodes
- auto-recovering from nodes that left or failed
- exactly-once processing in the face of node failures

Jet's core execution engine was designed for high throughput with low
system overhead. It uses a fixed-size thread pool to run any number of
parallel tasks. The foundational concept is a coroutine-like _tasklet_
that implements suspendable computation, allowing many of them to run
concurrently on a single thread.

Jet can integrate with many popular data storage systems and use them as
a source and sink:

- Apache Kafka
- Hadoop Distributed File System (and any Hadoop-compatible storage)
- Amazon S3
- Relational databases (or any other system with a JDBC driver)
- Stream changes from a database, through Change Data Capcture
- Message Queues
- Apache Avro files
- Hazelcast in memory data-structures like `IMap`, `ReplicatedMap`
and `IList`

On the data processing side, a key feature of Jet is _windowed
aggregation_, especially when combined with _event time-based
processing_. For example, if your data is GPS location reports, you
can find a smoothened velocity vector by using a _sliding window_ and
linear regression, and this takes just a few lines of code.

Jet takes care of the problem of reordered data (reports with an older
timestamp can lag and come in after newer ones) and it's able to produce
an uninterrupted stream of results (no time points skipped) even while
some cluster nodes fail or get added.

