---
title: Windowed Aggregation
id: windowing
---

As we've seen in the guide for
[Stateful Transformations](../api/stateful-transforms.md#aggregate)
aggregation is the cornerstone of distributed stream processing. Most of
the useful things we can achieve with stream processing need one or the
other form of aggregation.

By definition aggregation takes a **finite** set of data and produces
a result from it. In order to make it work with infinite streams, we
need some way to break up the stream into finite chunks. This is what
**windowing** does.

The criteria based on which windowing separates the chunks out of the
data is **time**, more accurately the timestamp of the input events (
most of the windows we will going to meet are actually "time windows").

For details on what timestamps are usually being used see the
[Streaming and Event Time](../concepts/event-time.md) section.

## Add Timestamps to the Stream

According to what we have discussed so far, in order to use aggregation
in an infinite stream, the first thing you need to do in Jet is to make
sure that your stream has timestamps.

As we have seen in the [Building Pipelines](../api/pipeline.md) section
you can obtain a
[SourceStreamStage](https://docs.hazelcast.org/docs/jet/latest-dev/javadoc/com/hazelcast/jet/pipeline/StreamSourceStage.html)
with a call like `pipeline.drawFrom(someStreamSource)`.

`SourceStreamStage` in turn offers various methods to assign
timestamps to your events. To just name a few:

* `withNativeTimestamps()` declares that the stream will use the
  source's native timestamps. This typically refers to the timestamps
  that the external source system sets on each event. Keep in mind
  that not all types of sources are capable to provide such native
  timestamps.

* `withIngestionTimestamp()` declares that the source will assign the
  time of observing the event, so the "processing time", as the event's
  timestamp.

We also have the option of not using timestamps at all (if we don't
intend to do any aggregation), or to use any arbitrary function to
extract a timestamp from the events. We can also specify an
"allowed lag" after which late events will be discarded. For further
details consult the
[javadoc](https://docs.hazelcast.org/docs/jet/latest-dev/javadoc/com/hazelcast/jet/pipeline/StreamSourceStage.html)
.

## Specify the Window

Let's illustrate how windows are set in Jet, via an example. Let's say
we want to analyse word frequencies in a bunch of tweets. First we need
a source stage:

```java
StreamStage<Tweet> tweets = p.readFrom(twitterStream())
      .withTimestamps(Tweet::timestamp, SECONDS.toMillis(5));
```

We have specified two things: how to extract the timestamp and how much
event "lateness" we want to tolerate. We said that any event we receive
can be at most five seconds behind the highest timestamp we have
already received. If it’s older than that, we’ll ignore it. On the flip
side, this means that we’ll have to wait for an event that has occurred
five seconds after the given window’s end before we can assume we have
all the data to emit that result. More about that in the
[section about early results](#get-early-results).

Now that we have the timestamps we can go ahead and specify the window:

```java
StageWithWindow<Tweet> windowedTweets = tweets
      .window(sliding(MINUTES.toMillis(1), SECONDS.toMillis(1)))
```

We have just told Jet to do our processing on events that have occurred
within the last minute and update the result every second. More about
available types of windows in [following sections](#types-of-windows).

## Apply the Aggregation

Having assigned timestamps to events and defined windows we can now add
the rest of our processing logic, including aggregations:

```java
windowedTweets
      .flatMap(tweet -> traverseArray(tweet.text().toLowerCase().split("\\W+")))
      .filter(word -> !word.isEmpty())
      .groupingKey(wholeItem())
      .aggregate(counting())
      .drainTo(Sinks.logger());
```

The `aggregate` operation we have called here is in fact the one defined
in
[StageWithKeyAndWindow](https://docs.hazelcast.org/docs/jet/latest-dev/javadoc/com/hazelcast/jet/pipeline/StageWithKeyAndWindow.html#aggregate-com.hazelcast.jet.aggregate.AggregateOperation1-)
. As windows become completed it emits `KeyedWindowResult` objects
containing the result of the aggregate operation (on the input items
that belong to the particular window) and the timestamp denoting the
window's ending time.

## Types of Windows

Jet supports following types of windows:

* [tumbling window](../api/stateful-transforms.md#tumblingwindow)
* [sliding window](../api/stateful-transforms.md#slidingwindow)
* [session window](../api/stateful-transforms.md#sessionwindow)

## Get Early Results

Results for windows are emitted when Jet considers the window closed.
This decision is made based on the "watermark". It is a timestamped item
Jet inserts into the stream that says "from this point on there will be
no more items with timestamp less than this".

The watermark could be as simple as "the most recent timestamp observed
in any event". Depending on the source it could be something more
sophisticated too, but in essence it is related to the "current value"
of (event) time.

Completion of a window being dependent on the watermark, window lengths
being potentially large, allowing for significantly late events can all
add up to long delays until we get some results. We may want to track
the progress of a window while it's still accumulating events.

You can tell Jet to give you, at regular intervals, the current status
on all the windows it has some data for, but aren’t yet complete. For
our example of trending words the window definition would need to change
like this:

```java
StageWithWindow<Tweet> windowedTweets = tweets
    .window(sliding(MINUTES.toMillis(1), SECONDS.toMillis(1))
        .setEarlyResultsPeriod(SECONDS.toMillis(1)))
```

The `KeyedWindowResult` objects we will get also have an
[isEarly()](https://docs.hazelcast.org/docs/jet/latest-dev/javadoc/com/hazelcast/jet/datamodel/WindowResult.html#isEarly--)
property which can be used to distinguish between early and final
results.
