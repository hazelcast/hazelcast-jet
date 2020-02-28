---
title: Fault Tolerance
description: Fault tolerance guarantees and options provided by Jet.
---

Jet keeps processing data without loss or duplication even if a node
fails, so it’s easy to build fault-tolerant data processing pipelines.
It uses a combination of several approaches to increase the resilience.

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

Hazelcast Jet stores the snapshots in an
[IMap](architecture/in-memory-storage.md) and does not have any external
dependency to an outside system. Data residing in the IMap are
replicated across the cluster to tolerate member failures. If a cluster
member fails, Jet uses the backup of the data that the member owned. Jet
also rebalances the snapshot data to keep the desired number of
replicas.

By default, Jet will make a single backup copy resulting in a system
that tolerates the failure of a single member at a time. You can tweak
this setting when starting Jet, for example increase the backup count to
two:

<!--DOCUSAURUS_CODE_TABS-->
<!--Programmatic Configuration-->

```java
JetConfig config = new JetConfig();
config.getInstanceConfig().setBackupCount(2);
JetInstance instance = Jet.newJetInstance(config);
```

<!--Declarative Configuration-->

```yaml
todo
```

<!--END_DOCUSAURUS_CODE_TABS-->

Note: if multiple members are lost simultaneously, some data from the
backing IMaps can be lost. This is not currently checked and the job
will restart with some state data from the snapshot missing, or it might
fail if classpath resources were added and are missing. We plan to
address this in future releases.

## Split-Brain Protection

A specific kind of failure is a so-called "split brain". It happens when
a network fails and a member or members think the other members left the
cluster, but in fact they still run, but don’t see each other over the
network. Now we have two or more fully functioning Jet clusters where
there was supposed to be one. Each one will recover and restart the same
Jet job, causing it to run multiple times.

Hazelcast Jet offers a mechanism to reduce this hazard: split-brain
protection. It works by ensuring that a job can be started only in a
cluster whose size is more than half of what it ever was. Enable
split-brain protection like this:

```java
jobConfig.setSplitBrainProtection(true);
```

If there’s an even number of members in your cluster, this may mean the
job will not be able to restart at all if the cluster splits into two
equally-sized parts. We recommend having an odd number of members.

Note also that you should ensure there is no split-brain condition at
the moment you are introducing new members to the cluster. If that
happens, both sub-clusters may grow to more than half of the previous
size. This will defuse the split-brain protection mechanism.

<!--  $$$ enterprise features $$$

### Disk Snapshot Storage

In-memory Snapshot Storage doesn’t cover the case when the entire
cluster must shut down.

The Lossless Cluster Restart allows you to gracefully shut down the
cluster at any time and have the snapshot data of all the jobs
preserved. After you restart the cluster, Jet automatically restores the
data and resumes the jobs.

Since the Hot Restart data is saved locally on each member, all the
members must be present after the restart for Jet to be able to reload
the data. Beyond that, there’s no special action to take: as soon as the
cluster re-forms, it will automatically reload the persisted snapshots
and resume the jobs.

## Exported Snapshots

In addition to regular snapshots, you can create exported snapshots. The
lifecycle of the exported snapshot is controlled by the user: it's
created upon user request and is stored in the cluster until the user
decides do remove it.

Exported snapshots are mainly used to upgrade the job: job is cancelled
with a snapshot and a new job is submitted that will use the saved
snapshot for initial state.
-->
