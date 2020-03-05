---
title: Stream Changes from IMap
---

[IMap](https://docs.hazelcast.org/docs/4.0/javadoc/com/hazelcast/map/IMap.html)
is the distributed data structure underlying Hazelcast. For Jet it's
also the most popular data source and sink.

The simplest scenario of using `IMap` as a source is when we treat the
entries it contains simply as a batch of data, read at a certain moment
of time. However Jet can make use of an `IMap` in a more advanced way,
treating all changes done to it (adding/updating/deleting entries) as
a stream of events which then can be processed further.

## Event Journal

In order to use an `IMap` as a change event stream source the first
thing that needs to be done is enabling its "event journal".

This is a matter of configuration (see also
[full guide](../operations/configuration.md) on
configuration), add following to your Jet member config, in your
 distribution
(`config/hazelcast.yaml` by default):

```yaml
hazelcast:
  map:
    name_of_map:
      event-journal:
        enabled: true
        capacity: 100000
        time-to-live-seconds: 10
```

Take care to obtain your Jet instance in code like this:

```java
JetInstance jet = Jet.bootstrappedInstance();
```

Specifying the _capacity_ is optional, defaults to _10,000_. Its
meaning is: the number of events the `IMap` will track before starting
to overwrite them. (The entire event journal is kept in RAM so you
should take care to adjust this value to match your use case.)

The _time-to-live_ is also optional, defaults to _0_ (meaning
“unlimited”, ie. events will be tracked until capacity is full, then
the oldest ones start to be overwritten). A non-zero TTL means that
events will be evicted after that amount of seconds, even if there is
still available capacity for tracking them.

## Example

Once the event journal has been configured setting up a source becomes
very simple. Let's see an example. A jet job which prints how many times
each key of an `IMap` has been updated in the past second.

Besides the job monitoring the map for changes (let's call it "the
consumer"), we will also run a second job actually doing the changes
to the map (let's call it "the producer").

```java
Pipeline producerPipeline = Pipeline.create();
producerPipeline.readFrom(TestSources.itemStream(100,
                        (ts, seq) -> ThreadLocalRandom.current().nextLong(0, 1000)))
                .withoutTimestamps()
                .map(l -> entry(l % 5, l))
                .writeTo(Sinks.map("myMap"));

Pipeline consumerPipeline = Pipeline.create();
consumerPipeline.readFrom(Sources.<Long, Long>mapJournal("myMap",
                        JournalInitialPosition.START_FROM_CURRENT))
                .withIngestionTimestamps()
                .window(WindowDefinition.tumbling(SECONDS.toMillis(1)))
                .groupingKey(Map.Entry::getKey)
                .aggregate(AggregateOperations.counting())
                .map(r -> String.format("Key %d had %d updates", r.getKey(), r.getValue()))
                .writeTo(Sinks.logger());


Job producerJob = jet.newJob(producerPipeline);
Job consumerJob = jet.newJob(consumerPipeline);

consumerJob.join();
```

The output produced is something like this (once per second):

```text
13:07:26.038 [ INFO] [c.h.j.i.c.W.loggerSink#0] Key 0 had 16 updates
13:07:26.038 [ INFO] [c.h.j.i.c.W.loggerSink#0] Key 1 had 25 updates
13:07:26.038 [ INFO] [c.h.j.i.c.W.loggerSink#0] Key 2 had 12 updates
13:07:26.039 [ INFO] [c.h.j.i.c.W.loggerSink#0] Key 3 had 15 updates
13:07:26.038 [ INFO] [c.h.j.i.c.W.loggerSink#0] Key 4 had 21 updates
```

## Considerations

As we see in this example, what we get from the map journal sorce is a
stream of `Map.Entry` elements , which get updated whenever there are
additions or updates in the underlying `IMap`.

If we care about deletions too, or we want to differentiate between
updates and additions, then there are further variants of this
Pipeline API method, which can be used. For details consult the
[javadoc](https://docs.hazelcast.org/docs/jet/4.0/javadoc/com/hazelcast/jet/pipeline/Sources.html#mapJournal-java.lang.String-com.hazelcast.jet.pipeline.JournalInitialPosition-com.hazelcast.function.FunctionEx-com.hazelcast.function.PredicateEx-).
