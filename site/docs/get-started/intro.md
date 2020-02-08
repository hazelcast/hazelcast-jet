---
title: Introduction
id: intro
---

Jet is a distributed data processing engine that treats the data as a
stream. It can process both static datasets (i.e., _batch jobs_) and
live event streams. It can compute aggregate functions over infinite
data streams by using the concept of windowing: dividing the stream into
a sequence of subsets (windows) and applying the aggregate function over
each window.

With unbounded event streams there arises the issue of failures in the
cluster. Jet can tolerate failures of individual
members without loss, offering the _exactly-once_ processing guarantee.

You deploy Jet to a cluster of machines and then submit your processing
jobs to it. Jet will automatically make all the cluster members
participate in processing your data. It can connect to a distributed
data source such as Kafka, Hadoop Distributed File System, or a Hazelcast
cluster. Each member of the Jet cluster will consume a slice of the
distributed data, which makes it easy to scale up to any level of throughput.