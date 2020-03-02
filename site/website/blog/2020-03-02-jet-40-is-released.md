---
title: Jet 4.0 is released
author: Can Gencer
authorURL: http://twitter.com/cgencer
authorImageURL: https://pbs.twimg.com/profile_images/1187734846749196288/elqWdrPj_400x400.jpg
---

We're happy to introduce the release of Jet 4.0 which brings several new
features. This release was a big effort and a total of 230 PRs were
merged, making it one of our biggest in terms of new features.

## Distributed Transaction supports

Jet previously had first-class support for fault tolerance through an
implementation of the Chandy-Lamport distributed snapshotting algorithm.
However, this requires participation from the whole pipeline, including
sources and sinks. Previously, the exactly-once processing guarantee was
only limited to replayable sources such as Kafka. Jet 4.0 comes with a
full two-phase-commit (2PC) implementation which makes it possible to
have end to end exactly processing with acknowledgement based sources
such as JMS. Jet is also now able to work with transactional sinks to
avoid duplicate writes, including a transactional file sink
implementation and Kafka.

We will have additional posts about this topic in the future.

## Python User Defined Function Support

Python is a popular language with a very large variety of libraries, and
is especially popular with data scientists. Jet offers a data processing
framework for both streams and batches of data, but the API for defining
the pipeline itself is currently limited to Java. To bridge this gap, in
this version we have added a native way to execute Python code within a
Jet pipeline, by internally making use of a gRPC bridge. The user is now
able to add a mapping stage which takes an input item, and transforms it
using a supplied Python function. The Python function can also work with
dependencies such as scikit and many others to apply Machine Learning
inference, for example:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.itemStream(10, (ts, seq) -> bigRandomNumberAsString()))
    .withoutTimestamps()
    .apply(mapUsingPython(new PythonServiceConfig()
            .setBaseDir(baseDir)
            .setHandlerModule("take_sqrt")))
    .writeTo(Sinks.observable(RESULTS));
```

The user only has to supply the following Python function:

```python
import numpy as np

def transform_list(input_list):
    """
    Uses NumPy to transform a list of numbers into a list of their square
    roots.

    :param input_list: the list with input items
    :return: the list with input items' square roots
    """
    num_list = [float(it) for it in input_list]
    sqrt_list = np.sqrt(num_list)
    return [str(it) for it in sqrt_list]
```

## Observables

When you submit a Jet pipeline, typically it reads the data from a
source and writes to a sink (such as a `IMap`). When the submitter of
the pipeline wants to read the results, the sink must be read outside of
the pipeline, which is not always very convenient.

In Jet 4.0, a new sink type called `Observable` is added which can be
used to publish messages directly to the caller. It utilizes a Hazelcast
Ringbuffer as the underlying data store.

```java
Observable<SimpleEvent> o = jet.newObservable();
o.addObserver(event -> System.out.println(event));
p.readFrom(TestSources.itemStream(10))
 .withoutTimestamps()
 .writeTo(Sinks.observable(o));
jet.newJob(o).join();
```

The `Observable` can also be used to be notified of a job's completion
and any errors that may occur during processing.

## Support for Custom Metrics

Over the last few releases we've been improving the metrics support in
Jet, such as being able to get metrics directly from running or
completed jobs through the use of `Job.getMetrics()`. In this release,
we've made it possible to also add your own custom metrics into a
pipeline through the use of a simple API:

```java
p.readFrom(TestSources.itemStream(10))
 .withoutTimestamps()
 .map(event -> {
     if (event.sequence % 2 == 0) {
         Metrics.metrics("numEvens").increment();
     }
     return event;
 }).writeTo(Sinks.logger());
```

These custom metrics will then be available as part of
`Job.getMetrics()` or through JMX along with the rest of the metrics.

## Debezium, Kafka Connect and Twitter Connectors

As part of Jet 4.0, we're release three new connectors:

### Debezium

Thew new Debezium connector integrates natively with Jet without
requiring Kafka and allows you to to stream changes from the likes of
MySQL, PostgreSQL and many others supported by the Debezium Project.

```java
Configuration configuration = Configuration
        .create()
        .with("name", "mysql-inventory-connector")
        .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
        /* begin connector properties */
        .with("database.hostname", mysql.getContainerIpAddress())
        .with("database.port", mysql.getMappedPort(MYSQL_PORT))
        .with("database.user", "debezium")
        .with("database.password", "dbz")
        .with("database.server.id", "184054")
        .with("database.server.name", "dbserver1")
        .with("database.whitelist", "inventory")
        .with("database.history.hazelcast.list.name", "test")
        .build();

Pipeline p = Pipeline.create();
p.readFrom(DebeziumSources.cdc(configuration))
 .withoutTimestamps()
 .map(record -> Values.convertToString(record.valueSchema(), record.value()))
 .writeTo(Sinks.logger());
```

### Kafka Connect

The Kafka Connect connectors allows you to use any existing Kafka
Connect source and use it natively with Jet, without requiring presence
of a Kafka Cluster. The records will be streamed as Jet events instead,
which can be processed further and it has full support for
fault-tolerance and replaying.

### Twitter

We've also released a simple source that uses the Twitter client, which
can be used to processing on a stream of Tweets.

```java
Properties credentials = new Properties();
properties.setProperty("consumerKey", "???"); // OAuth1 Consumer Key
properties.setProperty("consumerSecret", "???"); // OAuth1 Consumer Secret
properties.setProperty("token", "???"); // OAuth1 Token
properties.setProperty("tokenSecret", "???"); // OAuth1 Token Secret
List<String> terms = new ArrayList<>(Arrays.asList("term1", "term2"));
StreamSource<String> streamSource =
             TwitterSources.stream(credentials,
                     () -> new StatusesFilterEndpoint().trackTerms(terms)
             );
Pipeline p = Pipeline.create();
p.readFrom(streamSource)
 .withoutTimestamps()
 .writeTo(Sinks.logger());
```

## Improved Jet Installation

We've also made many improvements to the Jet installation package. It
has been cleaned up to reduce the size, and now supports the following:

* Default config format is now YAML and many of the common options are
  in the default configuration.
* A rolling file sink is now the default, which writes to the `log`
  folder
* Support for daemon mode through `jet-start -d` switch.
* Improved readme and a new "hello world" application which can be
  submitted right after installation.
* Improved JDK9+ support, to avoid illegal import warnings.

## Hazelcast 4.0

Undoubtedly one of the biggest changes in this release is that Jet is
now based on Hazelcast 4.0 - which in itself was a major release and
brought many new features and technical improvements such as improved
performance and Intel Optane DC Support and encryption at rest.

Going forward, we intend to make shorter, more frequent releases to
bring new features to our users quicker.

## Breaking Changes and Migration Guide

As part of 4.0, we've also done some house cleaning and as a result some
things have been moved around. All the changes are listed as part of the
[migration guide in the reference
manual](https://docs.hazelcast.org/docs/jet/latest-dev/manual/#migration-guides).

We are committed to backwards compatibility going forward and any
interfaces or classes which are subject to change will be marked as
`@Beta` or `@EvolvingApi` going forwards.