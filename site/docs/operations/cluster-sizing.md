---
title: Cluster Sizing
id: cluster-sizing
---

Jet cluster performance depends on multiple factors including the pipeline design and user defined functions. Therefore, planning the Jet cluster remains a complex task that requires a knowledge of the Jet architecture and concepts. We will introduce a basic guideline that will help you size your cluster. 

We recommend to always benchmark your setup before deploying it to production. See a sample cluster sizing with benchmark that can be used as a reference.

Please read the Hazelcast IMDG Deployment and Operations Guide when using the [local IMDG](concepts/in-memory-storage.md#relationship-with-hazelcast-imdg) setup. Your Jet cluster will run both workloads (local IMDG and Jet jobs) so you should plan for it.

## Sizing considerations

To size the cluster for your use case, you must first be able to answer the following questions:

* What are the throughput and latency requirements?
* How many concurrent Jobs shall the cluster run?
* Fault tolerance requirements
* Characteristics of the data (number of input partitions, key distribution and , size of the dataset)
* Shape of the pipelines (operations used, external systems involved)
* Source and sink capacity
* Network throughput 
* How long is the error window?

## Determining cluster size

Even a single Jet instance can host and run hundreds of jobs at a time. The clustered setup improves the performance (throughput and latency) of hosted jobs and increases the resillience.

A cluster with 3 members is a minimum count for fault tolerant operations. Generally, you need ```n+1``` cluster members to tolerate `n` member failures with next higher odd number choosen for a split brain detection.

Jet can utilise hundreds of CPU cores efficiently by exploiting data and task parallelism. Adding more members to the cluster therefore helps with scaling the CPU-bound computations. Better performance is achieved by distributing the data partitions accros the cluster to process them in parallel. 

Benchmark your jobs in a clustered setup to see the differences in performance, see the Sizing Example.

## Sizing for failures

Jet cluster is elastic to deal with failures and performance spikes. Mostly down-scales temporarily reduce the available resources and increase the stress on remaining members. The overal memory and CPU available in the cluster reduces. The data previously owned by the newly offline member is distributed among the remaining members. The cluster must catch up the missed data in the stream and keep up with the head of the stream.

To tolerate the failure of one member, we recommend to size your cluster to operate with ```n-1``` members setup.

You can use Hazelcast IMap Journal to ingest the streaming data. Journal is a in-memory structure with a fixed capacity. If the jobs consuming the journal can't keep up there is a risk of data loss.  The pace of the data producers and the capacity of the Journal therefore determine the lenght of an error window of your application. If you can't afford losing data, consider increasing the journal size or use persistent streaming storage such as Apache Kafka or Apache Pulsar.

## Balancing cluster size with job count

The jobs running in one cluster share the resources to maximise the HW utilization. This is efficient for setups without a risk of noisy neighbours such as:
* Clusters hosting many short-living jobs
* Clusters hosting jobs with a predictable performance 
* Jobs with relaxed SLAs

For stronger resource isolation (multi-tenant environments, strict SLAs), consider starting multiple smaller clusters with resources (CPU, memory, network) allocated on an OS level or a resource manager level.

## Hardware Planning

Jet is designed to run efficiently on homogeneous clusters. All JVM processes that participate in the cluster should have equal CPU, memory and network resources. One slow cluster member can kill the performance of the whole cluster.

### Minimal Configuration

Jet is a lightweight framework and is reported to run on a devices such as Rapsberry Pi Zerro (1GHz single-core CPU, 512MB RAM).

### Recommended Configuration

As a starting point for a data-intensive oprerations consider machines with: 

* 8 CPU cores
* 16 GB RAM
* 10 Gbps network

### CPU

Jet can utilise hundreds of CPU cores efficiently by exploiting data and task parallelism. Adding more CPU can therefore help with scaling the CPU-bound computations. Read about the [Execution model](architecture/execution-engine.md) to understand how Jet makes the computation parallel and design your pipelines according to it. 

Don't rely just on a CPU usage when benchmarking your cluster, benchmnark the throughput and latency instead. The task manager of Jet [can be configured](https://hazelcast.com/blog/idle-green-threads-in-jet/) to use the CPU aggresivelly. The idle Jet instance can use 20% of the CPU, Jet using 100% of the CPU can handle 5x more load.

### Memory

Jet is a memory-centric framework and all operational data must fit to the memory. This design leads to a predictable performance but requires enough RAM not to run out of memory. Estimate the memory requirements and plan with a headroom of 25% for normal memory fragmentation. For fault-tolerant operations, we recommend reserving an extra memory to survive the failure. See [Sizing for failures](#sizing-for-failures).

Memory consumption is affected by:

* **JVM:** Hundreds of MBs (depending on your JVM)
* **Jet framework overhead:** Tens of MBs
* **Resources deployed with your job:** Considerable when attaching big files such as models for ML inference pipelines.
* **State of the running jobs:** Varies as it's affected by the shape of your pipeline and by the data being processed. Most of the memory is consumed by operations that aggregate and buffer data. Typically the state also scales with the number of distinct keys seen within the time window. Learn how the operations in the pipeline store its state. Operators comming with Jet provide this information in the javadoc.
* **State back-up:** For jobs configured as fault-tolerant, the state of the running jobs is regularly snapshotted and saved in the cluster. Cluster keeps two consecutive snapshots at a time (old one is kept until the new one is successfully created). Both current and previous snapshot can be saved in multiple replicas to increase data safety. The memory required for state back-up can be calculated as ```(Snapshot size * 2 * Number of replicas) / Cluster member count```. The snapshot size is displayed in the Management Center. You might want to keep some state snapshots residing in the cluster as points of recovery, so plan the memory requirements accordingly.
* **Data stored in the [local IMDG](concepts/in-memory-storage.md#relationship-with-hazelcast-imdg)**: Any data hosted in the local IMDG. Notably the IMap Journal to store the streaming data. See the [Hazelcast IMDG Deployment and Operations Guide](https://hazelcast.com/resources/hazelcast-deployment-operations-guide/). Mostly Journal.


### Network

Jet uses the network internally to shuffle data and to replicate the back-ups. Network is also used to read input data from and to write results to remote systems or to do RPC calls when enriching. In fact, lot of Jet Jobs are network bound. Using a 10 Gigabit or higher network can improve application performance.

Consider colocating Jet cluster with the data source to avoid moving data back and forth over the wire. Processed results are are often aggregated, so the size is reduced.

Jet is designed to run in a single LAN. Deploying Jet cluster to a network with high or varying latencies leads to unpredictable performance.

### Disk

Jet is an in-memory framework. Cluster disks aren't involved in regular operations except for logging and thus are not critical for the cluster performance. 

Consider using more performant disks when:

* You use the cluster filesystem as a source or sink - faster disks improve the performance
* Using disk persistence for [Lossless Cluster Restart](https://docs.hazelcast.org/docs/jet/latest/manual/#configure-lossless-cluster-restart-enterprise-only)


## Benchmarking and Sizing Examples

### Setup

### Cluster size and performance

### Memory sizing


