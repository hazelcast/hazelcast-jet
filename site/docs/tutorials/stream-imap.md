---
title: Stream changes from IMap
id: stream-imap
---

[IMap](https://docs.hazelcast.org/docs/latest-dev/javadoc/com/hazelcast/map/IMap.html)
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
[full guide](https://jet-start.sh/docs/operations/configuration) on
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

<details>
  <summary>There is also a simpler solution for embedded mode!</summary>

  When using Jet in the embedded mode the same effect can be achieved
  more simply, via programmatic configuration:

  ```java
  JetConfig cfg = new JetConfig();
  cfg.getHazelcastConfig()
     .getMapEventJournalConfig("name_of_map")
     .setEnabled(true)
     .setCapacity(1000)         // how many events to keep before evicting
     .setTimeToLiveSeconds(10); // evict events older than this
  JetInstance jet = Jet.newJetInstance(cfg);
  ```

</details>

## Source

Once the event journal has been configured the source can be set up like
this:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.mapJournal("name_of_map", JournalInitialPosition.START_FROM_OLDEST));
```

What we get is a stream of `Map.Entry` elements, which get updated
whenever there are additions or updates in the underlying `IMap`.

If we care about deletions too, or we want to differentiate between
updates and additions, then there are further variants of this
Pipeline API method, which can be used. For details consult the
[javadoc](https://docs.hazelcast.org/docs/jet/latest-dev/javadoc/com/hazelcast/jet/pipeline/Sources.html#mapJournal-java.lang.String-com.hazelcast.jet.pipeline.JournalInitialPosition-com.hazelcast.function.FunctionEx-com.hazelcast.function.PredicateEx-).
