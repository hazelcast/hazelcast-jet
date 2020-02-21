---
title: Processing Guarantees
id: processing-guarantees
---

With unbounded stream processing comes the challenge of forever
maintaining the continuity of the output, even in the face of changing
cluster topology. A Jet node may leave the cluster due to an internal
error, loss of networking, or deliberate shutdown for maintenance. In
this case Jet must suspend the computation, re-plan it for the smaller
cluster, and then resume in such a way that the state of computation
remains intact. For example, if you have a `counting` aggregation step,
for the count to remain correct the aggregating task must see each item
exactly once. The technical term for this is the *exactly-once
processing guarantee* and Jet supports it, however it requires support
from all the participants in the computation, including data sources,
sinks, and side-inputs.

Hazelcast Jet also supports a lesser guarantee, *at-least-once*, in
which an item can be observed more than once. It allows more performance
for those pipelines that can gracefully deal with duplicates.
