---
title: Processing Guarantees for Stateful Computation
id: processing-guarantees
---

With unbounded stream processing comes the challenge of forever
maintaining the continuity of the output, even in the face of changing
cluster topology. A Jet node may leave the cluster due to an internal
error, loss of networking, or deliberate shutdown for maintenance. In
this case Jet must suspend the computation, re-plan it for the smaller
cluster, and then resume in such a way that the state of computation
remains intact.

## Stateless vs. Stateful Computation

A given computation step can be either stateless or stateful. Basic
examples of the former are `map`, `filter` and `flatMap`. Jet doesn't
have to take any special measures to make stateless transforms work in a
fault-tolerant data pipeline because they are pure functions and always
give the same output for the same input.

The challenges come with stateful computation, and most real-world
pipelines contain at least one such transform. For example, if you have
a `counting` windowed aggregation step, for the count to remain correct
the aggregating task must see each item exactly once. The technical term
for this is the *exactly-once processing guarantee* and Jet supports it.
It also supports a lesser guarantee, *at-least-once*, in which an item
can be observed more than once. It allows more performance for those
pipelines that can gracefully deal with duplicates.

## Requirements on Outside Resources

Upholding a processing guarantee requires support not just from Jet, but
from all the participants in the computation, including data sources,
sinks, and side-inputs. Exactly-once processing is a cross-cutting
concern that needs careful systems design.

Ideally, the data sources will be *replayable:* observing an item
doesn't consume it and you can ask for it again later, after restarting
the pipeline. Jet can also make do with *transactional* sources that
forget the items you consumed, but allow you to consume them inside a
transaction that you can roll back.

We have a symmetrical requirements on the data sink: ideally it is
*idempotent*, allowing duplicate submission of the same data item
without resulting in duplication, but Jet can also work with
*transactional* sinks.
