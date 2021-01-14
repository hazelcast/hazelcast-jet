---
title: Building Pipelines
description: How to build processing pipelines in Jet.
---

The general shape of any data processing pipeline is `readFromSource ->
transform -> writeToSink` and the natural way to build it is from source
to sink. The `Pipeline` API follows this
pattern. For example,

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.items("the", "quick", "brown", "fox"))
 .map(item -> item.toUpperCase())
 .writeTo(Sinks.logger());
```

In each step, such as `readFrom` or `writeTo`, you create a pipeline
_stage_. The stage resulting from a `writeTo` operation is called a
_sink stage_ and you can't attach more stages to it. All others are
called _compute stages_ and expect you to attach further stages to them.

## BatchStage and StreamStage

The API differentiates between batch (bounded) and stream (unbounded)
sources and this is reflected in the naming: there is a
[BatchStage](/javadoc/{jet-version}/com/hazelcast/jet/pipeline/BatchStage.html)
and a
[StreamStage](/javadoc/{jet-version}/com/hazelcast/jet/pipeline/StreamStage.html)
, each offering the operations appropriate to its kind.
Depending on the data source, your pipeline will end up starting with a
batch or streaming stage. A batch source still can be used to simulate
a streaming source using the `addTimestamps` method, which will
convert it into a `StreamStage`.

In this section we'll mostly use batch stages, for simplicity, but the
API of operations common to both kinds is identical. Jet internally
treats batches as a bounded stream. We'll explain later on how to apply
windowing, which is necessary to aggregate over unbounded streams.

### Adding Timestamps to a Stream

The Pipeline API guides you to set up the timestamp policy right after
you obtain a source stage. `pipeline.readFrom(someStreamSource)` returns
a `SourceStreamStage` which offers just these methods:

- `withNativeTimestamps()`
  declares that the stream will use source's native timestamps. This
  typically refers to the timestamps that the external source system
  sets on each event.

- `withTimestamps(timestampFn)`
  provides a function to the source that determines the timestamp of
  each event.

- `withIngestionTimestamps()`
  declares that the source will assign the time of ingestion as the
  event timestamp. System-time semantics will be used.

- `withoutTimestamps()`
  declares that the source stage has no timestamps. Use this if you
  don't need them (i.e., your pipeline won't perform windowed
  aggregation or stateful mapping).

Exceptionally, you may need to call `withoutTimestamps()` on the source
stage, then perform some transformations that determine the event
timestamps, and then call `addTimestamps(timestampFn)` to instruct Jet
where to find them in the events. Some examples include an enrichment
stage that retrieves the timestamps from a side input or flat-mapping
the stream to unpack a series of events from each original item. If you
do this, however, it will no longer be the source that determines the
watermark.

#### Issue with Sparse Events

If the time is extracted from the events, time progresses only when
newer events arrive. If the events are sparse, the time will effectively
stop until a newer event arrives. This causes high latency for
time-sensitive operations (such as window aggregation). The time is also
tracked for every source partition separately and if just one partition
has sparse events, time progress in the whole job is hindered.

To overcome this you can either ensure there's a consistent influx of
events in every partition, or you can use `withIngestionTimestamps()`
which doesn't have this issue because it's based on system clock on the
member machines.

#### Preserving Event Order

As mentioned above, Jet models the data processing job as a pipeline.
The pipeline consists of stages and each stage (except sources) accepts
the events from upstream stages, processes the events, and passes the
transformed events to the downstream stage. Jet creates multiple
tasklets (also called coroutine) for each stage to parallelize the
processing. Data is transferred among tasklets of subsequent stages
using various routing strategies. Ordering of events depends on the
selected strategy.

The default strategy is round-robin. This means that an event emited by
the upstream tasklet can be routed to any tasklet of consecutive
downstream stage. This strategy helps with shaping the data traffic by
evenly distributing the data flow among processors. We get the best
performance with it. However, it is not possible to preserve the event
ordering with this round-robin data transfer strategy. Events can be
processed in order that doesn't match the source.

If the pipeline is stateless or has no order-dependent stateful logic,
the change of the event order will not affect the processing outcome.
Maintaining the order in such pipelines does not matter so the default
routing strategy works well.

Outcome of pipelines with order-dependent stateful logic depends on
event ordering. An example is a pattern recognition or complex event
processing pipelines. Also, external services that a pipeline interacts
with can be stateful and their state can also be order dependent.

For those cases, `Pipeline` has a `preserveOrder`property. Enabling this
property instructs Jet to keep the order of events with the same
partitioning key. Each partition will follow an unique data transfer
path from source to sink.

You can enable this property as follows:

```java
Pipeline p = Pipeline.create();
p.setPreserveOrder(true);
```

Enabling this property negatively impacts job performance. Creating a
unique path from the source to the sink for each partition applies a
restriction to the data flow and the most important effect of this
restriction is the inability to increase tasklet parallelism while
advancing the stages (except the stages that create a new partitions).
Since this applies from the source, we can say that the source
parallelism determines the parallelism on all pipeline stages. For
example, if the pipeline has a single partitioned source, then every
stage is affected by this low parallelism of the source. If the source
is partitioned, there is not much decrease in performance since the job
starts with high parallelism in source.

> Note that: Changing the partition keys in the different stages of the
pipeline may cause out-of-order.

#### Prefer Assigning Timestamps at the Source

In some source implementations, especially partitioned ones like Kafka,
there is a risk of high [event time skew](../concepts/event-time)
occurring across partitions as the Jet processor pulls the data from
them. This problem is especially pronounced when Jet recovers from a
failure and restarts your job. In this case Jet must catch up with all
the events that arrived since taking the last snapshot. It will receive
these events at the maximum system throughput and thus each partition
will have a lot of data each time Jet polls it. This means that the
interleaving of data from different partitions will become much more
coarse-grained and there will be sudden jumps in event time at the
points of transition from one partition to the next. The jumps can
easily exceed the configured `allowedLag` and cause Jet to drop whole
swaths of events as late.

In order to mitigate this issue, Jet has special logic in its
partitioned source implementations that keeps separate track of the
timestamps within each partition and knows how to reconcile the
temporary differences that occur between them.

This feature works only if you set the timestamping policy in the source
using `withTimestamps()` or `withNativeTimestamps()`.

## Multiple Inputs

Your pipeline can consist of multiple sources, each starting its own
pipeline branch, and you are allowed to mix both kinds of stages in the
same pipeline. You can merge the branches with joining transforms such
as [hash-join](stateless-transforms.md#hashjoin),
[co-group](stateful-transforms.md#co-group--join) or
[merge](stateless-transforms.md#merge).

As an example, you can merge two stages into one by using the `merge`
operator:

```java
Pipeline p = Pipeline.create();

BatchSource<String> leftSource = TestSources.items("the", "quick", "brown", "fox");
BatchSource<String> rightSource = TestSources.items("jumps", "over", "the", "lazy", "dog");

BatchStage<String> left = p.readFrom(leftSource);
BatchStage<String> right = p.readFrom(rightSource);

left.merge(right)
    .writeTo(Sinks.logger());
```

## Multiple outputs

Symmetrically, you can fork the output of a stage and send it to more
than one destination:

```java
Pipeline p = Pipeline.create();
BatchStage<String> src = p.readFrom(TestSources.items("the", "quick", "brown", "fox"));
src.map(String::toUpperCase)
   .writeTo(Sinks.files("uppercase"));
src.map(String::toLowerCase)
   .writeTo(Sinks.files("lowercase"));
```

## Pipeline lifecycle

The pipeline itself is a reusable object which can be passed around and
submitted several times to the cluster. To execute a job, you need the
following steps:

1. Create an empty pipeline definition
1. Start with sources, add transforms and then finally write to a sink.
   A pipeline without any sinks is not valid.
1. Create or obtain a `JetInstance` (using either embedded instance,
   bootstrapped or a client)
1. Using `JetInstance.newJob(Pipeline)` submit it to the cluster
1. Wait for it complete (for batch jobs) using `Job.join()` or just let
   it run on the cluster indefinitely, for streaming jobs.

## Types of Transforms

Besides sources and sinks, Jet offers several transforms which can be used
to process data. We can divide these into roughly the following categories:

1. [Stateless transforms](stateless-transforms.md): These transforms do
 not have any notion of _state_ meaning that all items must be processed
 independently of any previous items. Examples:
 [map](stateless-transforms.md#map),
 [filter](stateless-transforms.md#filter),
 [flatMap](stateless-transforms.md#flatmap),
 [hashJoin](stateful-transforms.md#)
1. [Stateful transforms](stateful-transforms.md): These transforms
 accumulate data and the output depends on previously encountered items.
 Examples: [aggregate](stateful-transforms.md#aggregate),
 [rollingAggregate](stateful-transforms.md#rollingaggregate),
 [distinct](stateful-transforms.md#distinct),
 [window](stateful-transforms.md#window)

This distinction is important because any stateful computation requires
the state to be saved for fault-tolerance and this has big implications
in terms of operational design.
