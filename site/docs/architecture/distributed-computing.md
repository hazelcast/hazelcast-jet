---
title: Distributed Execution
id: distributed-computing
---

In this section we will take a deep dive into the fundamentals of
distributed computing and Jet's specific take on it. You'll find that
having some intuition and insight into how distributed computing
actually works in Jet makes a big difference when diagnosing your
pipeline and improving its performance.

Jet performs high performance in-memory data processing by modeling the
computation as a _Directed Acyclic Graph (DAG)_, where vertices
represent computation and edges represent data flows. A vertex receives
data from its inbound edges, performs a step in the computation, and
emits data to its outbound edges. Both the edge and the vertex are
distributed entities: there are many parallel instances of the
`Processor` type that perform a single vertex's computation work on
each cluster member. An edge between two vertices is implemented with
many data connections, both within a member (concurrent SPSC queues) and
between members (Hazelcast network connections).

One of the major reasons to divide the full computation task into
several vertices is _data partitioning_: the ability to split the data
stream into slices which can be processed in parallel, independently of
each other. This is how Jet can parallelize and distribute the
_group-and-aggregate_ stream transformation, the major workhorse in
distributed computing. To make this work, there must be a function which
computes the _partitioning key_ for each item and makes all related
items map to the same key. Jet can then route all such items to the same
processor instance, but has the freedom to route items with different
keys to different processors.
