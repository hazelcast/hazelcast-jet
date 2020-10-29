---
title: 015 - Avoiding Event Reordering Effects
description: Avoid event reordering in unexpected cases
---

*Since*: 4.4

## Goal

Avoid non-intuitive event reordering in Jet pipelines.

## Problem statement

Jet processes events in parallel, therefore a total order of events
doesn't exist. However, in some use cases, users expect the order of
events with the same key to be preserved, in order to be able to apply
stateful processing logic on them.

So far, Jet's default has been to use a round-robin strategy to balance
the traffic across parallel processors. Typically, the pipeline starts
out with a source that has low parallelism, and the round-robin edge
spreads out the data to downstream transforms with full parallelism.

It is also possible to spread out the data using a partitioning scheme,
so that all events with the same key go to the same downstream
processor. This is less flexible and may suffer from data bias, where a
handful of keys dominate the traffic volume. On the other hand, its
advantage over round-robin is preserving the order among the same-keyed
events.

Jet already uses partitioning by key, but only on edges towards stages
that explicitly use a grouping key, such as `aggregate`. The aggregating
processor must observe all the events with a given key, but it is not
ordering-sensitive.

Jet also has the `mapStateful` transform, which is more general than
`aggregate` and can contain arbitrary stateful logic. This logic is
often order-sensitive, so it breaks down under event reordering.

![Events Getting Reordered](assets/events_getting_reordered.svg)

In this document we describe ways to avoid the usage of round-robin
edges and still preserve the performance potential of the previous
implementation.

## Design Ideas

There are two basic ways to remove event reordering:

* Prevent it from happening in the first place
* Restore it before encountering an order-sensitive transform (stage)

These approaches have different effects on the performance: the first
one constrains our freedom to balance the traffic, while the second one
introduces a sorting overhead.

The second approach is not feasible most of the time because we don't
have a good enough sorting key. The obvious choice, event timestamp, is
not good enough because there's nothing stopping two events from
occurring within the same millisecond.

Therefore we're focusing on the first approach: keep maintaining the
original event order at every stage. Let us analyze the situation at
each stage, starting from the source. There are two kinds of data
sources:

1. single-point source (no parallelism)
2. partitioned source (parallelized by a grouping key)

If we start out from a single-point source and there's no grouping key
we can extract and parallelize on, we have no choice but to process all
the data without parellelism. There is no technical challenge to solve
here, so we'll focus on the cases where we do have a
grouping/partitioning key.

## Keep the Order of a Partitioned Source

A partitioned source is both parallel and order-preserving, it preserves
the order of the keyed substreams. The current situation in Jet is such
that it loses this order in stateless transform stages, through
round-robin edges. Also, Jet doesn't automatically capture the
partitioning key and propagate it through the pipeline.

### Keep it Without the Key

We can keep the order even without the partitioning key, simply by
keeping the partitions isolated throughout the pipeline. This seems to
allow us a level of parallelism equal to the number of partitions in
the source. However, it is pretty inflexible:

1. Jet's symmetrical execution model means that the source parallelism
  must be a multiple of the size of the cluster
2. Jet cluster may change in size, affecting the parallelism of the
  source
3. Number of partitions in the source is not intended to drive Jet's
   parallelism

### Keep it Using the Key

In streaming pipelines we already wrap every event object into a
`JetEvent` that holds the metadata. We can add the partitioning key (or
just the partition ID, an integer) there and then apply a partitioned
edge whenever we used to apply the round-robin one. We can apply an
optimization as well, by using the cheaper `isolated` edge when
connecting transforms with the same local parallelism.

This approach is more intrusive because it affects the pipelines without
windowed aggregation, which currently don't need the wrapping
`JetEvent`.

## Decision: Keep the Order Without the Key

TODO: explain the decision.

## Implementation of the Preserve Parallelism Approach

This affects the code in the `Planner`. If the incoming edge we would
attach to the transform is a round-robin one (`unicast`), then:

* Ensure that the local parallelism (LP) of the input vertex of the
  transform is equal to the PlannerVertex of the upstream transform.

* Connect these transform vertices with isolated edge.

Otherwise, if it's a partitioned edge, do nothing.

We applied the policy to enforce equal local parallelism during the
pipeline-to-DAG conversion stage, `Pipeline.toDag()`. We also inspected
and adjusted the code in each of the `Transform.addToDa()`
implementations, switching to the `isolated` edge where needed. Here is
the summary of these changes:

|Transform or Operator|The summary of changes|
|------|------|
|Map/Filter/FlatMap|Enforce parallelism equal to the upstream, apply the `isolated` edge.|
|Custom (Core API) Transform|Enforce parallelism equal to the upstream, apply the `isolated` edge.|
|Partitioned Custom Transform|No changes, it already uses a partitioned edge.|
|Aggregation|No changes. Aggregation is order-insensitive, and creates a new order in its output. Marked as `OrderCreator`.|
|Distinct|No changes. We don't guarantee to emit the very first distinct item.|
|Sorting|No changes. Sorting is order-insensitive and enforces its own order in the output. Marked as a `OrderCreator`.|
|HashJoinTransform| Edge-0 (carrying the stream to be enriched): Enforce parallelism equal to the upstream, apply the `isolated` edge.|
|Stateful Mapping|No changes, stateful mapping already preserves the order of the upstream stage. Marked as `OrderSensitive`|
|MergeTransform| Enforce parallelism equal to the minimum of its upstreams, apply the updated version of isolated edge.|
|TimestampTransform|No changes, this transform already uses the `isolated` edge. Marked `OrderSensitive` to keep doing what it already does.|
|PeekTransform|No changes.|
|Sources|No changes.|
|Sinks| At the first, all sinks are marked as `OrderSensitive` because it is easier to implement, but some sinks may not really be. TODO: we should consider and mark each sink according to their order sensitivities for a better job planning, and provide an interface for the users to mark their sinks during creation.|

## Smart Job Planning

We classified the transforms as order-sensitive and order-insensitive.
This allows us to analyze the graph of the pipeline and identify which
parts of it need the ordering restrictions. Basically, we must protect
the order only on a path going from an order-creating stage to a
stateful mapping stage. We also classified the transforms into
order-propagating and order-creating. Sources create the order, as well
as sorting and aggregating stages. For example, if a pipeline contains
stateful mapping downstream of an aggregating stage, only that part must
preserve the order.

The algorithm to find these order-sensitive subgraphs requires us to
traverse the `Transform` DAG in reverse topological order. This way, at
each stage, we know whether somewhere in its downstream there's an
order-sensitive stage. Here's the algorithm we use:

1. Traverse to DAG in the reverse topological order.
2. When visiting an order-sensitive transform, activate the ordering
   prevention logic for this and future transforms until visiting an
   order-creating transform.
3. After visiting an order-creating transform, deactivate the
   ordering prevention logic for the future transforms until visiting an
   order-sensitive node.
4. Follow this procedure to visit all nodes.

## Not Implementing Now: Sorting Events Between Consecutive Watermarks

Jet already offers support for watermarking. If we have a sorting key
for events, we can sort the events between the two consecutive watermark
according to the sorting key so we can put the events in their initial
order. Users will not define the window in an explicit way. The user
will only add stateful mapping (or any order-sensitive stage) to his
pipeline, and we will add such an intermediate sorting stage during
planning.

As the cost of this work, sorting events requires an extra computation
and we have to wait for the closing watermark to arrive. This increases
the latency with watermarkPeriod/2 on average. The issue of sparse
events can also occur in this approach.

To use this solution, we need a sorting key such as more precise
timestamp or mark like a SequenceId for events- Our timestamp precision
is low, resulting in overlapping events with the same timestamp and it
is not easy to add this SequenceId (sorting key) to events in a reliable
way especially when the total parallelism of the source is greater than
one. I just put this approach to be seen.
