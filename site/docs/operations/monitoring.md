---
title: Monitoring and Metrics
description: Options available for monitoring the health and operations of Jet clusters and jobs.
---

## Logging

>Under construction!

## Metrics

Jet exposes various metrics to facilitate monitoring of the cluster
state and of running jobs.

Metrics have associated **tags** which describe which object the metric
applies to. The tags for job metrics typically indicate the specific
[DAG vertex](../concepts/dag.md) and processor the metric belongs to.

Each metric instance provided belongs to a particular Jet cluster
member, so different cluster members can have their own versions of the
same metric with different values.

The metric collection runs in regular intervals on each member, but note
that the metric collection on different cluster members happens at
different moments in time. So if you try to correlate metrics from
different members, they can be from different moments of time.

There are two broad categories of Jet metrics. For clarity we will group
them based on significant tags which define their granularity.

Last but not least let’s not forget about the fact that each Jet member
is also a [Hazelcast](https://github.com/hazelcast/hazelcast) member, so
Jet also exposes all the metrics available in Hazelcast too.

Let’s look at these 3 broad categories of metrics in detail.

### Hazelcast Metrics

There is a wide range of metrics and statistics provided by Hazelcast:

* statistics of distributed data structures (see [Reference Manual](https://docs.hazelcast.org/docs/4.0/manual/html-single/index.html#getting-member-statistics))
* executor statistics (see [Reference Manual](https://docs.hazelcast.org/docs/4.0/manual/html-single/index.html#executor-statistics))
* partition related statistics (state, migration, replication)
* garbage collection statistics
* memory statistics for the JVM which current IMDG member belongs to
  (total physical/free OS memory, max/committed/used/free heap memory
  and max/committed/used/free native memory)
* network traffic related statistics (traffic and queue sizes)
* class loading related statistics
* thread count information (current, peak and daemon thread counts)

### Cluster-wide Metrics

>Under construction!

### Job-specific Metrics

>Under construction!

### Exposing Metrics

>Under construction!

#### Over JMX

>Under construction!

#### Via Job API

>Under construction!

### Configuration

>Under construction!

## Management Center

>Under construction!
