---
title: Lossless Cluster Restart 
description: TODO
---

The Lossless Cluster Restart feature allows you to gracefully shut down
the cluster at any time and have the snapshot data of all the jobs
preserved. After you restart the cluster, Jet automatically restores the
data and resumes the jobs.

This allows Jet cluster to be shut down gracefully and restarted
without a data loss.

## Persistence

Jet regurlarly[snapshots](/docs/architecture/fault-tolerance) it's
state. The state snapshot can be used as a recovery point. Snapshots are
stored in memory. Lossless Cluster Restart works by persisting the
RAM-based snapshot data. It uses the
[Hot Restart Persistence](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#hot-restart-persistence)
feature of Hazelcast. See the
[docs](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#hot-restart-persistence)
for detailed description including fine-tuning.

## Fault Tolerance and Maintentance Windows

Lossless Cluster Restart is a maintenance tool. It isn't a means of
fault tolerance.

The purpose of the Lossless Cluster Restart is to provide a maintenance
window for member operations and restart the cluster in a fast way. As
a part of the ordered shutdown process, Jet makes sure that the
in-memory snapshot data were persisted safely.

The fault tolerance of Jet is entirely RAM-based. Snapshots is stored in
multiple in-memory replicas across the cluster. Given the redundancy
present in the cluster, this is sufficient to maintain a running cluster
across single-node failures (or multiple-node, depending on the backup
count), but it doesn’t cover the case when the entire cluster must shut
down.

This design decision allows Jet to exclude disk from the operations that
affect Job execution, leading to low and predictable latency.

## Performance considerations

Even with the fastest solid-state storage, Lossless Cluster Restart
reduces the maximum snapshotting throughput available to Jet:
while DRAM can take up to 50 GB/s, a high-end storage device caps at
2 GB/s. Keep in mind that this is throughput per member, so even on a
minimal cluster of 3 machines, this is actually 6 GB/s available to Jet.
Also, this throughput limit does not apply to the data going through the
Jet pipeline, but only to the periodic saving of the state present in
the pipeline. Typically the state scales with the number of distinct
keys seen within the time window.

## Configuration

To use Lossless Cluster Restart, enable it in hazelcast-jet.yaml:

```yml
hazelcast-jet:
  instance:
    lossless-restart-enabled: true
```

To set the base directory where the data will be stored, configure Hot
Restart in the IMDG configuration:

```yml
hazelcast:
  hot-restart-persistence:
    enabled: true
    base-dir: /mnt/hot-restart
```

Quick programmatic example:

```java
JetConfig cfg = new JetConfig();
cfg.getInstanceConfig().setLosslessRestartEnabled(true);
cfg.getHazelcastConfig().getHotRestartPersistenceConfig()
        .setEnabled(true)
        .setBaseDir(new File("/mnt/hot-restart"))
        .setParallelism(2);
```

To have the cluster shut down gracefully, as required for this feature
to work, do not kill the Jet instances one by one. As you are killing
them, the cluster starts working hard to rebalance the data on the
remaining members. This usually leads to out-of-memory failures and the
loss of all data.

The entire cluster can be shut down gracefully in several ways:

<!--DOCUSAURUS_CODE_TABS-->

<!--Command line-->

```bash
jet-cluster-admin -o shutdown
```

<!-- Jet Management Center -->

![Cluster Shutdown using Management Center](assets/management-center-shutdown.png)

<!-- Programmatically -->

```java
jet.getCluster().shutdown();
```

<!--END_DOCUSAURUS_CODE_TABS-->

The first two ways make use of Jet’s REST endpoint, which isn’t enabled
by default. You can enable it by setting the property

```bash
hazelcast.rest.enabled=true
```

This can be either a system property (`-Dhazelcast.rest.enabled=true`)
or a property in the Hazelcast Jet configuration:

<!--DOCUSAURUS_CODE_TABS-->

<!--YAML -->

```yaml
hazelcast-jet:
  properties:
    hazelcast.rest.enabled: true
```

<!-- Programmatically -->

```java
JetConfig config = new JetConfig();
config.getProperties().setProperty("hazelcast.rest.enabled", "true");
```

<!--END_DOCUSAURUS_CODE_TABS-->

Since the Hot Restart data is saved locally on each member, all the
members must be present after the restart for Jet to be able to reload
the data. Beyond that, there’s no special action to take: as soon as the
cluster re-forms, it will automatically reload the persisted snapshots
and resume the jobs.