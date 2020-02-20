---
title: Stateful Transforms
id: stateful-transforms
---

Stateful transforms refer to those computations which typically
accumulate some data and emit results depending on what was encountered
before.

For example, we want to counti how many items are encountered so far in
a stream, and emit the current count with every new item. This is quite
distinct from the mapping transforms explored in the previous section
because after each item, we need to maintain a current _state_ of the
number of total items encountered so far.

When it comes to maintaining state, there is also a big distinction
between streaming and batch pipelines. Windowing only
applies to streaming pipelines where an element of time is present and
applying a one-time aggregation over the whole data set is only possible
in batch pipelines.

## aggregate

Data aggregation is the cornerstone of distributed stream processing. It
computes an aggregate function (simple examples: sum or average) over
the data items.

When used without a defined [window](#window), `aggregate` applies a
one-time aggregation over the whole of the input which is only possible
in a bounded input (using `BatchStage`).

For example, a very simple aggregation will look as follows:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.items(0, 1, 2, 3, 4, 5))
 .aggregate(AggregateOperations.counting())
 .writeTo(Sinks.logger());
```

This will output only one result, which is the count of all the items:

```text
11:49:12.435 [ INFO] [c.h.j.i.c.W.loggerSink#0] 6
```

The definition of the aggregate operation hides behind the
`AggregateOperations.counting()` method call. This is a static method in
our AggregateOperations utility class, which provides you with some
predefined aggregate operations. Jet provides several built in
aggregations such as:

|operation|description|
|---------|:----------|
|`averagingLong/Double`|Calculates an average of the given inputs|
|`counting`|Returns the count of all the items|
|`summingLong/Double`|Returns the sum of all the items|
|`maxBy/minBy`|Finds the minimum or maximum sorted according to some criteria|
|`toList`|Simply groups the items in a list and returns it|
|`bottomN/topN`|Calculates the bottom or top N items sorted according to some criteria.|
|`linearTrend`|Computes a trend line over the given items, for example the velocity given GPS coordinates|
|`allOf`|Combine multiple aggregations into one aggregation (for example, if you want both sum and average)|

For a complete list, please refer to the `AggregateOperations` class.
You can also implement your own aggregate operations using the builder
in `AggregateOperation`.

### groupingKey

Typically you don’t want to aggregate all the items together, but
classify them by some key and then aggregate over each group separately.
This is achieved by using the `groupingKey` transform and then applying
an aggregation on it afterwards.

We can extend the previous example to group odd and even values
separately:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.items(0, 1, 2, 3, 4, 5))
 .groupingKey(i -> i % 2 == 0 ? "odds" : "evens")
 .aggregate(AggregateOperations.counting())
 .writeTo(Sinks.logger());
```

```text
11:51:46.723 [ INFO] [c.h.j.i.c.W.loggerSink#0] odds=3
11:51:46.723 [ INFO] [c.h.j.i.c.W.loggerSink#0] evens=3
```

Grouping is critical for aggregating massive data sets in distributed
computing - otherwise you would not able to make use of parallelization
as effectively.

## rollingAggregate

Rolling aggregation is similar to aggregate but instead of waiting to
output until all items are received, it outputs _one item per input
item_. Because of this, it's possible to use it in a streaming pipeline
as well as the aggregation is applied in a continous way. The same
pipeline from [aggregate](#aggregate), can be rewritten to use a
`rollingAggregate` transform instead:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.items(0, 1, 2, 3, 4, 5))
 .groupingKey(i -> i % 2 == 0 ? "odds" : "evens")
 .rollingAggregate(AggregateOperations.counting())
 .writeTo(Sinks.logger());
```

Instead of a single line of output, we would get the following output
instead:

```text
12:06:29.405 [ INFO] [c.h.j.i.c.W.loggerSink#0] odds=1
12:06:29.405 [ INFO] [c.h.j.i.c.W.loggerSink#0] odds=2
12:06:29.405 [ INFO] [c.h.j.i.c.W.loggerSink#0] odds=3
12:06:29.406 [ INFO] [c.h.j.i.c.W.loggerSink#0] evens=1
12:06:29.406 [ INFO] [c.h.j.i.c.W.loggerSink#0] evens=2
12:06:29.406 [ INFO] [c.h.j.i.c.W.loggerSink#0] evens=3
```

## window

The process of data aggregation takes a finite batch of data and
produces a result. We can make it work with an infinite stream if we
break up the stream into finite chunks. This is called windowing and
it’s almost always defined in terms of a range of event timestamps (a
time window).

Window transforms requires a stream which is annotated with
_timestamps_, that is each input item has a timestamp associated to
it. Timestamps are given in milliseconds and are general represented in
_epoch_ format as a simple `long`.

For a more in-depth look at Jet's event time model, please refer to the
[Event Time](../concepts/event-time) section.

The general way to assign windows a stream works as follows:

### tumblingWindow

Tumbling windows are the most basic window type - a window of constant
size that "tumbles" along the time axis. If you use a window size of 1
second, Jet will group together all events that occur within the same
second and you’ll get window results for intervals [0-1) seconds, then
[1-2) seconds, and so on.

A simple example is given below:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.itemStream(100)) // will emit 100 items per second
 .withIngestionTimestamps()
 .window(WindowDefinition.tumbling(TimeUnit.SECONDS.toMillis(1)))
 .aggregate(AggregateOperations.counting())
 .writeTo(Sinks.logger());
```

When you run this pipeline, you should see the following output, where
each output window is marked with start and end timestamps:

```text
14:26:28.007 [ INFO] [c.h.j.i.c.W.loggerSink#0] WindowResult{start=14:26:27.000, end=14:26:28.000, value='100', isEarly=false}
14:26:29.009 [ INFO] [c.h.j.i.c.W.loggerSink#0] WindowResult{start=14:26:28.000, end=14:26:29.000, value='100', isEarly=false}
14:26:30.004 [ INFO] [c.h.j.i.c.W.loggerSink#0] WindowResult{start=14:26:29.000, end=14:26:30.000, value='100', isEarly=false}
14:26:31.008 [ INFO] [c.h.j.i.c.W.loggerSink#0] WindowResult{start=14:26:30.000, end=14:26:31.000, value='100', isEarly=false}
```

As with a normal aggregation, it's also possible to apply a grouping to
a windowed operation:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.itemStream(100)) // will emit 100 items per second
 .withIngestionTimestamps()
 .groupingKey(i -> i.sequence() % 2 == 0 ? "even" : "odd")
 .window(WindowDefinition.tumbling(TimeUnit.SECONDS.toMillis(1)))
 .aggregate(AggregateOperations.counting())
 .writeTo(Sinks.logger());
```

In this mode, the output would be keyed:

```text
15:09:24.017 [ INFO] [c.h.j.i.c.W.loggerSink#0] KeyedWindowResult{start=15:09:23.000, end=15:09:24.000, key='odd', value='50', isEarly=false}
15:09:24.018 [ INFO] [c.h.j.i.c.W.loggerSink#0] KeyedWindowResult{start=15:09:23.000, end=15:09:24.000, key='even', value='50', isEarly=false}
15:09:25.014 [ INFO] [c.h.j.i.c.W.loggerSink#0] KeyedWindowResult{start=15:09:24.000, end=15:09:25.000, key='odd', value='50', isEarly=false}
15:09:25.015 [ INFO] [c.h.j.i.c.W.loggerSink#0] KeyedWindowResult{start=15:09:24.000, end=15:09:25.000, key='even', value='50', isEarly=false}
15:09:26.009 [ INFO] [c.h.j.i.c.W.loggerSink#0] KeyedWindowResult{start=15:09:25.000, end=15:09:26.000, key='odd', value='50', isEarly=false}
15:09:26.009 [ INFO] [c.h.j.i.c.W.loggerSink#0] KeyedWindowResult{start=15:09:25.000, end=15:09:26.000, key='even', value='50', isEarly=false}
15:09:27.013 [ INFO] [c.h.j.i.c.W.loggerSink#0] KeyedWindowResult{start=15:09:26.000, end=15:09:27.000, key='odd', value='50', isEarly=false}
```

### slidingWindow

Sliding window is like a tumbling window that instead of hopping from
one time range to another, slides along instead. It slides in discrete
steps that are a fraction of the window’s length. If you use a window of
size 1 second sliding by 100 milliseconds, Jet will output window
results for intervals [0.00-1.00) seconds, then [0.10-1.1) seconds, and
so on.

We can modify the tumbling window example as below:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.itemStream(100)) // will emit 100 items per second
 .withIngestionTimestamps()
 .window(WindowDefinition.sliding(TimeUnit.SECONDS.toMillis(1), 100))
 .aggregate(AggregateOperations.counting())
 .writeTo(Sinks.logger());
```

When you run this pipeline, you should see the following output where
you can see that the start and end timestamps of the windows are overlapping.

```text
15:07:38.108 [ INFO] [c.h.j.i.c.W.loggerSink#0] WindowResult{start=15:07:37.100, end=15:07:38.100, value='100', isEarly=false}
15:07:38.209 [ INFO] [c.h.j.i.c.W.loggerSink#0] WindowResult{start=15:07:37.200, end=15:07:38.200, value='100', isEarly=false}
15:07:38.313 [ INFO] [c.h.j.i.c.W.loggerSink#0] WindowResult{start=15:07:37.300, end=15:07:38.300, value='100', isEarly=false}
15:07:38.408 [ INFO] [c.h.j.i.c.W.loggerSink#0] WindowResult{start=15:07:37.400, end=15:07:38.400, value='100', isEarly=false}
15:07:38.505 [ INFO] [c.h.j.i.c.W.loggerSink#0] WindowResult{start=15:07:37.500, end=15:07:38.500, value='100', isEarly=false}
```

### sessionWindow

Session window captures periods of activity followed by periods of
inactivity. You define the "session timeout", i.e., the length of the
inactive period that causes the window to close. An example of a
session window is for example a specific user's activity on a website.
Typically this would be followed by bursts of activity (while the user
is browsing website) followed by rather long periods of inactivity.

As with other aggregate transforms, if you define a grouping key, there
is a separate, independent session window for each key.

In the example below, we want to find out how many different events each
user had during a web session. The data source is a stream events read
from Kafka and we assume that the user session is closed after 15
minutes of inactivity:

```java
p.readFrom(KafkaSources.kafka("website-events", ..))
 .withIngestionTimestamps()
 .groupingKey(event -> event.getUserId())
 .window(WindowDefinition.session(TimeUnit.MINUTES.toMillis(15)))
 .aggregate(AggregateOperations.counting())
 .writeTo(Sinks.logger());
```

## distinct

Suppresses duplicate items from a stream. If you apply a grouping key,
two items mapping to the same key will be duplicates. This operation
applies primarily to batch streams, but also works on a windowed
unbounded stream.

This example takes some input of integers and outputs only the distinct
values:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.items(0, 1, 1, 2, 3, 4, 5, 6))
 .distinct()
 .writeTo(Sinks.logger());
```

We can also use `distinct` with grouping, for example the following will
only string which have different first letters:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.items("joe", "john", "jenny", "maria"))
 .groupingKey(s -> s.substring(0, 1)
 .distinct();
```

The `distinct` operator can be used for batch and streaming pipelines,
but requires a window to be applied in a streaming pipeline.

## mapStateful

mapStateful is an extension of the simple [map](stateless-transforms#map)
transform and adds the capability to optionally retain mutable state.

The major use case of stateful mapping is recognizing a pattern in the
event stream, such as matching start-transaction with end-transaction
events based on an event correlation ID. More generally, you can
implement any kind of state machine and detect patterns in the input of
any complexity.

As with other stateful operations, you can also use a `groupingKey` to
have a unique state per key.

For example, consider a pipeline that matches incoming
`TRANSACTION_START` events to `TRANSACTION_END` events which can arrive
unordered and when both are received outputs how long the transaction
took.

This would be difficult to express in terms of a `slidingWindow`,
because we can't know how long a transaction would take in advance, and
if it would span multiple windows. It can't be expressed using
`sessionWindow` either, because we don't want to wait until the window
times out before emitting the results.

Let's say we have the following class:

```java
public class TransactionEvent {
    long timestamp();
    String transactionId();
    EventType type();
}

public enum EventType {
    TRANSACTION_START,
    TRANSACTION_END
}
```

We can then use the following `mapStateful` transform to match start
and events:

```java
p.readFrom(KafkaSources.kafka("transaction-events", ..))
 .withNativeTimestamps(0)
 .groupingKey(event -> event.transactionId())
 .mapStateful(MINUTES.toMillis(10),
   () -> new TransactionEvent[2],
   (state, id, event) -> {
        if (event.type() == TRANSACTION_START) {
            state[0] = event;
        } else if (event.type() == TRANSACTION_END)
            state[1] = event;
        }
        if (state[0] != null && state[1] != null) {
            // we have both start and end events
            entry(transactionId, state[1].timestamp() - state[0].timestamp())
        }
        // we have only one event, do nothing for now.
        return null;
    },
    (state, id, currentWatermark) ->
        // if we have not received both events after 10 minutes, we will emit a timeout entry
        (state[0] == null || state[1] == null)
            ? entry(id, TIMED_OUT)
            : null
 ).writeTo(Sinks.logger());
```

You will note that we also had to set an expiry time on the events,
otherwise would eventually run out of memory as we accumulate more and
more transactions.

## co-group / join

Co-grouping allows to join any number of inputs on a common key, which
can be anything you can calculate from the input item. This makes it
possible to correlate data from two or more diferrent sources. In the
same transform you are able to apply an aggregate function to all the
grouped items.

As an example, we can use a sequence of events that would be typical on
a e-commerce website: `PageVisit` and `AddToCart`. We want to find how
many visits were required before an item was added to the cart. For
simplicity, let's say we're working with historical data and we are
processing this data from a set of logs.

```java
Pipeline p = Pipeline.create();
BatchStageWithKey<PageVisit, Integer> pageVisit =
    p.readFrom(Sources.files("visit-events.log", ..))
    .groupingKey(event -> event.userId());
BatchStageWithKey<AddToCart, Integer> addToCart =
    p.readFrom(Sources.files("cart-events.log", ..))
     .groupingKey(event -> event.userId());
```

After getting the two keyed streams, now we can join them:

```java
BatchStage<Entry<Integer, Tuple2<Long, Long>>> coGrouped = pageVisit
        .aggregate2(counting(), addToCart, counting());
```

This gives an item which contains the counts for both events for the
same user id. From this, it's easy to calculate the ratio of visits vs
add to cart events.

Co-grouping can also be applied to windowed streams, and works exactly
the same as `aggregate` option. An important consideration is that the
timestamps from both streams would be considered, so it's important that
the two streams don't have widely different timestamps.
