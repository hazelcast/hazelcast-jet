---
title: In-Memory Storage and Correctness
description: How Jet makes use of Hazelcast's in-memory storage
---

A distinctive feature of Hazelcast Jet is that it has no dependency on
disk storage, it keeps all of its operational state in the RAM of the
cluster. It also doesn't delegate its fault tolerance concerns to an
outside system like ZooKeeper. This comes from the fact that it's built
on top of Hazelcast IMDG.

## Level of Safety

Jet backs up the state to its own `IMap` objects. `IMap` is a replicated
in-memory data structure, storing each key-value pair on a configurable
number of cluster members. By default it makes a single backup copy,
resulting in a system that tolerates the failure of a single member at a
time. The cluster recovers its safety level by re-establishing all the
missing backups, and when this is done, another node can fail without
data loss. You can tweak the backup count in the configuration, for
example:

```java
JetConfig config = new JetConfig();
config.getInstanceConfig().setBackupCount(2);
JetInstance instance = Jet.newJetInstance(config);
```

If multiple members fail simultaneously, some data from the backing
`IMap`s can be lost. Jet detects this by counting the entries in the
snapshot `IMap`, so it won't run a job  with missing data.

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
