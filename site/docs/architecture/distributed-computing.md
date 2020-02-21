---
title: Distributed Execution
id: distributed-computing
---

Jet models the computations as a [Directed Acyclic Graphs
(DAG)](concepts/dag.md), where vertices represent computation and edges
represent data flows.

## Planner

Jet is programmed using the [Pipeline API](api/pipeline.md). After the
Pipeline is submitted for execution, the Hazelcast Jet planner converts
it into a Directed Acyclic Graph (DAG). The DAG is distributed and
replicated to the entire cluster and executed in parallel on each
member.

![Job Deployment](assets/dag-distribution.png)

For each DAG vertex, Jet planner determines how many parallel instances,
or processors, will run per cluster member - the local parallelism. This
varies as some operations can be parallelized while others can not.

Reading from a file for example can't be parallelized as the parallel
instances would compete for the single resource, bringing no performance
benfit. Jet therefore creates just one instance of the file reader per
cluster member. Transforming a record is an operation that can be
parallelized trivially. Jet creates as many transformer instances as
many CPU cores the cluster member has.

The planner then connects the processors using edges. The edges mediate
the data flow and route the data. Data routing strategy is determined
based on the pipeline.

In the previous example, the parallelism of the upstream processor (file
reader) and downstream process (mapper) varies. Jet will fan out records
from the file reader, picking a random mapper instance for each record
to balance the load. If the partitioning of the data changes, edges
route the data so that all records in a respective partition end up on a
single downstream processor instance. This may require shuffling data
over the network.

Jet prints the DAG to a system log when initializing the Job, including
the local parallelism and the edge routing strategies.

```digraph
digraph DAG {
    "file-connector" [localParallelism=1];
    "map" [localParallelism=4];
    "mapUsingPartitionedServiceAsync" [localParallelism=4];
    "map-stateful-keyed" [localParallelism=4];
    "mapWithUpdatingSink(cache)" [localParallelism=4];
    "file-connector" -> "map" [queueSize=1024];
    "map" -> "mapUsingPartitionedServiceAsync" [label="distributed-partitioned", queueSize=1024];
    "mapUsingPartitionedServiceAsync" -> "map-stateful-keyed" [label="distributed-partitioned", queueSize=1024];
    "map-stateful-keyed" -> "mapWithUpdatingSink(cache)" [label="partitioned", queueSize=1024];
}
```

## Continuous Execution

The processors run concurrently. Each processor continuously consumes
data from the upstream edges and publishes the results downstreams. The
capacity of the edge is limited to keep the amount of in-flight data
under control and to provide natural back pressure. An overloaded
processor isn't consuming more records from the input edges so they
eventually become full. Full edges prevent upstream processors from
producing more records so they idle for a while, giving the downstream
processor time and a CPU to catch up.