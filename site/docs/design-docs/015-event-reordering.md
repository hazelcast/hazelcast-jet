---
title: 015 - Avoiding Event Reordering Effects
description: Avoid event reordering in unexpected cases
---

*Since*: 4.4

## Goal

Avoid non-intuitive event reordering in Jet pipelines.

## Problem statement

Events are getting reordered inside Jet jobs. Thus, Jet only enables the
users to write jobs that are insensitive to the encounter order:
jobs including only stateless mapping and aggregation based on
commutative-associative functions etc. But, this out-of-order affects
the correctness of jobs including state.

Jet execution engine tries to increase data parallelism as much as
possible while sending the events originating from one source DAG node
to multiple DAG nodes. This is why event disordering happens.

![Events Getting Reordered](assets/events_getting_reordered.svg)

After these events are distributed to multiple nodes, the occurrence
order of events is partially disrupted. After these disruption, if these
events encounter an order-sensitive transform(operator), they will cause
unexpected results.

## Design

There are two possible solutions for this problem:

- To prevent event reordering from happening
- To sort the events in the same window before they encounter the
  order-sensitive transforms(operators)

Both approaches have different effects on the performance. To make the
best of both of them, I drafted a design which is rather a hybrid
approach, in which we apply these approaches interchangeably during the
pipeline. We decide which one to use, according to the definition of the
associated part of the pipeline.

The main tradeoff we need to consider when creating this design is
allowing event reordering vs performance.

### Prevention of Event Reordering

When distributing events from a one node to multiple nodes, events get
out of order. If we specifically avoid this situation that breaks the
order in the pipeline, the order of events will be preserved. But, this
comes with the loss of parallelism: The maximum number of nodes in one
stage is restricted by the predecessor stage (LP of the stage <= LP of
the previous stage). Applying this for the entire pipeline may not
always be feasible. E.g. If any stage of the pipeline contains only a
single node (LP=1), following stages have to contain a single node,
which is a suboptimal utilization of parallelism.

### Implementation Details

In order to prevent the order of events from changing, we avoid the use
of round-robin edge. The common pattern we've implemented in most
transforms to achieve this described below.

If a transform does not use partitioned edge:

- Ensure that the LP of the input vertex of the transform is equal to
  the PlannerVertex of the upstream transform.
- Connect these transform vertices with isolated edge.

Otherwise:

- do not change the properties of the transform if it already uses
  partitioned edge.

To ensure such LP equality between transforms, we had to specify LP's in
job planning (pipeline.toDag()) stage. After that, we did this by
modifying pipeline stages (transforms) one-by-one. The changes applied
to the transforms listed below:

|Transform or Operator|The summary of changes|
|------|------|
|Aggregate Transform (Both of Single and Two Stage)|No changes have been made to the vertex's local parallelism of this transform and the configuration of the edge which connects it to the previous stage (Without considering non commutative-associative aggregates). We mark the transform as SequencerTransform to understand that these aggregate transforms produce their own order during job planning.|
|Batch Source Transform|No changes have been made to the vertex's local parallelism of this transform.|
|Distinct Transform|No changes have been made to the vertex's local parallelism of this transform and the configuration of the edge which connects it to the previous stage.|
|FlatMapTransform|If the preserve ordering is active, we determine the LP of the transform vertex to have the same LP as the PlannerVertex of the upstream transform and connect them with the isolated edge.|
|FlatMapStatefulTransform|No changes have been made to the vertex's local parallelism of this transform and the configuration of the edge which connects it to the previous stage. We mark this transform as OrderSensitiveTransform.|
|GlobalMapStatefulTransform|No changes have been made to the vertex's local parallelism of this transform and the configuration of the edge which connects it to the previous stage. We mark this transform as OrderSensitiveTransform. |
|GroupTransform(GroupAggregateTransform)|No changes have been made to the vertex's local parallelism of this transform and the configuration of the edge which connects it to the previous stage (Without considering non commutative-associative aggregates). We mark this transform as SequencerTransform.|
|HashJoinTransform|`TODO: Consider it in more detail.` With my little knowledge, I don't plan to make any changes to this transform.|
|MapTransform|If preserve ordering is active, we determine the LP of the transform vertex to have the same LP as the PlannerVertex of the upstream transform and connect them with the isolated edge.|
|KeyedMapStatefulTransform|No changes have been made to the vertex's local parallelism of this transform and the configuration of the edge which connects it to the previous stage. We mark this transform as OrderSensitiveTransform.|
|MergeTransform|`TODO: Consider it in more detail.`|
|PartitionedProcessorTransform|No changes have been made to this transform.|
|PeekTransform|No changes have been made to this transform.|
|ProcessorTransform|If preserve ordering is active, we determine the LP of the transform vertex to have the same LP as the PlannerVertex of the upstream transform and connect them with the isolated edge.|
|SortTransform|No changes have been made to the vertex's local parallelism of this transform and the configuration of the edge which connects it to the previous stage. We mark this transform as SequencerTransform. |
|SinkTransform|No changes have been made to the vertex's local parallelism of this transform and the configuration of the edge which connects it to the previous stage.|
|Stream Source Transform|No changes have been made to the vertex's local parallelism of this transform.|
|TimestampTransform|We mark this transform as OrderSensitiveTransform to keep its current characteristics after the change in the job planning phase.|
|WindowAggregateTransform|No changes have been made to the vertex's local parallelism of this transform and the configuration of the edge which connects it to the previous stage(Without considering non commutative-associative aggregates). We mark this transform as SequencerTransform.|
|WindowGroupTransform(WindowedGroupAggregateTransform)|No changes have been made to the vertex's local parallelism of this transform and the configuration of the edge which connects it to the previous stage. We mark this transform as SequencerTransform.|

### Sorting Events Between The Consecutive Watermarks

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

### Smart Job Planning

If we classify the transforms as order-sensitive and order-insensitive,
we can determine whether we will require the initial event order at the
stage of the pipeline. In other words, grouping the transforms according
to whether the result of the transforms is affected by the event order
or not is one of the most important factor that will help us hide the
ordering effects. To clarify why this would be useful, consider this
example case: Suppose the pipeline contains all order-insensitive
transforms. Then, if we are aware that they are order-insensitive, we
can avoid sorting the events unnecessarily. In this way, we make use of
parallelism as much as possible.

Similarly, if we know that a transform explicitly reorder the events, we
can skip any ordering related effort after this transform, even if the
next operation is order-sensitive. It would be unreasonable to sort
events from now on as the order of events after this explicit
order-breaking operation, will already get mixed up. As opposed to this,
some kind of transforms can put the events in a certain order. If there
is no order-sensitive transform before this type of transform, we don't
need to do any ordering related effort until we get here since the
user's explicit order will take effect from now on. These kind of
transforms fix the order that could be previously broken. (e.g. sort)
This group may include aggregation transforms since they make the
effects of reordering disappear (the output of windowed aggregate
transforms are ordered).

With the smart job planning, we determine the subpipelines that we need
to prevent reordering and where we add the sorting stages independently
from the user by considering the classes of transforms. Since we have to
consider the subsequent stages for any stage while making this
determination, it is not enough to visit the pipeline stages only with
topological order. We can with using reverse topological order.

By browsing on the reverse topological order, we can decide where to
activate/deactivate the ordering prevention logic. The algorithm for
this job will be as follows:

1. Traverse to DAG in the reverse topological order.
2. When visit a order-sensitive node (OrderSensitiveTransform), activate
   the ordering prevention logic for this and future nodes until
   visiting a node (SequencerTransform) that produces its own order.
3. After visiting the node that produces its own order, deactivate the
   ordering prevention logic the following nodes until visiting a
   order-sensitive node.
4. Follow this procedure to visit all nodes.

Until now, I have not considered sorting the items between watermarks. I
think to consider it according to its feasibility
