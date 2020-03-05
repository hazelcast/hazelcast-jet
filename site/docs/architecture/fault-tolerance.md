---
title: Fault Tolerance
description: Fault tolerance guarantees and options provided by Jet.
---

Hazelcast Jet doesn't delegate its cluster management and fault
tolerance concerns to an outside system like ZooKeeper. This comes from
the fact that it's built on top of Hazelcast IMDG, which has already
dealt with these concerns.

Jet keeps processing data without loss or duplication even if a node
fails so it’s easy to build fault-tolerant data processing pipelines.
It uses a combination of several approaches to improve resilience.

## Consistency

Jet takes snapshots of the entire state of the computation at regular
intervals. It coordinates the snapshot across the cluster and
synchronizes it with the data source. The source must ensure that, in
case of a restart, it will be able to re-emit all the data it emitted
after the last snapshot. Each of the other components in the job will
restore its processing state to exactly what it was at the time of the
last snapshot. If a cluster member goes away, Jet will restart the job
on the remaining members, restore the state of processing from the last
snapshot, and then seamlessly continue from that point.

Jet utilizes the [Chandy-Lamport distributed snapshotting
algorithm](http://lamport.azurewebsites.net/pubs/chandy.pdf) to offer
fault tolerance and processing guarantees.

The snapshots are part of the regular Jet operations. If you configure
your job as Exactly-once or At-least once, Jet automatically creates
snapshots in regular intervals. Jet manages the lifecycle of the
snapshots - the snapshot is automatically replaced by a next successful
snapshot.

## Data Safety

### In-Memory Snapshot Storage

Jet backs up the state to its own `IMap` objects. `IMap` is a replicated
in-memory data structure, storing each key-value pair on a configurable
number of cluster members. By default it makes a single backup copy,
resulting in a system that tolerates the failure of a single member at a
time. The cluster recovers its safety level by re-establishing all the
missing backups, and when this is done, another node can fail without
data loss. You can set the backup count in the configuration, for
example:

```yaml
hazelcast-jet:
  instance:
    backup-count: 2
```

If multiple members fail simultaneously, some data from the backing
`IMap`s can be lost. Jet detects this by counting the entries in the
snapshot `IMap` and it won't run a job with missing data.

## Split-Brain Protection

There is a special kind of cluster failure, popularly called the "Split
Brain". It occurs due to a complex network failure (a network
_partition_) where the graph of live connections among cluster nodes
falls apart into two islands. In each island it seems like all the other
nodes failed, so the remaining cluster should self-heal and continue
working. Now you have two Jet clusters working in parallel, each running
all the jobs on all the data.

Hazelcast Jet offers a mechanism to mitigate this risk: split-brain
protection. It works by ensuring that a job can be restarted only in a
cluster whose size is more than half of what it was before the job was
suspended. Enable split-brain protection like this:

```java
jobConfig.setSplitBrainProtection(true);
```

If there’s an even number of members in your cluster, this may mean the
job will not be able to restart at all if the cluster splits into two
equally-sized parts. We recommend having an odd number of members.

Note also that you should ensure there is no split-brain condition at
the moment you are introducing new members to the cluster. If that
happens, both sub-clusters may grow to more than half of the previous
size, circumventing split-brain protection.

<!-- ### Disk Snapshot Storage -->

<!-- In-memory Snapshot Storage doesn’t cover the case when the entire
cluster must shut down. -->

<!-- The Lossless Cluster Restart allows you to gracefully shut down the
cluster at any time and have the snapshot data of all the jobs
preserved. After you restart the cluster, Jet automatically restores the
data and resumes the jobs. -->

<!-- Since the Hot Restart data is saved locally on each member, all the
members must be present after the restart for Jet to be able to reload
the data. Beyond that, there’s no special action to take: as soon as the
cluster re-forms, it will automatically reload the persisted snapshots
and resume the jobs. -->

<!-- ## Exported Snapshots -->

<!-- In addition to regular snapshots, you can create exported
snapshots. The lifecycle of the exported snapshot is controlled by
the user: it's created upon user request and is stored in the cluster
until the user decides do remove it. -->

<!--
Exported snapshots are mainly used to upgrade the job: job is cancelled
with a snapshot and a new job is submitted that will use the saved
snapshot for initial state.  -->
