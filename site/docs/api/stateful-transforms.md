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
computing - otherwise you are not able to make use of parallelization.

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

### tumblingWindow

### slidingWindow

### sessionWindow

## distinct

## mapStateful

## co-group
