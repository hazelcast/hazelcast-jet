# Hazelcast Jet Code Samples

A repository of code samples for Hazelcast Jet. The samples show you how
to use the Pipeline API to solve a range of use cases, how to integrate
Jet with other systems and how to connect to various data sources (both
from a Hazelcast IMDG and 3rd-party systems). There is also a folder with
samples using the Core API.

## Stream Aggregation

### [Sliding Window](sliding-windows/src/main/java/StockExchange.java)
  - apply a sliding window
  - perform basic aggregation (counting)
  - print the results on the console

### [Sliding Window with Nested Aggregation](sliding-windows/src/main/java/TopNStocks.java)
  - like the above, plus:
  - add a second-level aggregation stage to find top/bottom N results

### [Session Window](session-windows/src/main/java/SessionWindow.java)
  - apply a session window
  - use a custom Core API processor as the event source
  - perform a composite aggregate operation (apply two aggregate functions
    in parallel).
  - print the results on the console

### [Early Window Results](early-window-results/src/main/java/TradingVolumeOverTime.java)
  - use the `SourceBuilder` to create a mock source of trade events from a
  stock market
  - apply a tumbling window, configure to emit early results
  - aggregate by summing a derived value
  - present the results in a live GUI chart

### [Rolling Aggregation](rolling-aggregation/src/main/java/TradingVolume.java)
  - use `SourceBuilder` to create a mock source of trade events from a stock
    market
  - simple rolling aggregation (summing the price)
  - keep updating the target map with the current values of aggregation
  - present the results in a live GUI chart

## Batch Aggregation
### [Word Count](wordcount/src/main/java/WordCount.java)
  - use an `IMap` as the data source
  - stateless transforms to clean up the input (flatMap + filter)
  - perform basic aggregation (counting)
  - print a table of the most frequent words on the console

### [Inverted Index with TF-IDF Scoring](tf-idf/src/main/java/TfIdf.java)
  - serialize a small dataset to use as side input
  - fork a pipeline stage into two downstream stages
  - stateless transformations to clean up input
  - count distinct items
  - group by key, then group by secondary key
  - aggregate to a map of (secondary key -> result)
  - hash-join the forked stages
  - open an interactive GUI to try out the results

## Joins
### [Co-Group and Aggregate](co-group/src/main/java/BatchCoGroup.java)
  - co-group three bounded data streams on a common key
  - for each distinct key, emit the co-grouped items in a 3-tuple of lists
  - store the results in an `IMap` and check they are as expected
### [Windowed Co-Group and Aggregate](co-group/src/main/java/WindowedCoGroup.java)
  - use the Event Journal of an `IMap` as a streaming source
  - apply a sliding window
  - co-group three unbounded data streams on a common key
  - print the results on the console
### Hash Join
  - see [here](#enrich-using-hash-join)

## Data Enrichment
### [Enrich Using IMap](enrichment/src/main/java/Enrichment.java)
  - the sample is in the `enrichUsingIMap()` method
  - use the Event Journal of an `IMap` as a streaming data source
  - apply the `mapUsingIMap` transform to fetch the enriching data from
    another `IMap`
  - enrich from two `IMap`s in two `mapUsingIMap` steps
  - print the results on the console
### [Enrich Using ReplicatedMap](enrichment/src/main/java/Enrichment.java)
  - the sample is in the `enrichUsingReplicatedMap()` method
  - use the Event Journal of an `IMap` as a streaming data source
  - apply the `mapUsingReplicatedMap` transform to fetch the enriching data
    from another `IMap`
  - enrich from two `ReplicatedMap`s in two `mapUsingReplicatedMap` steps
  - print the results on the console
### [Enrich From External System with Async API](enrichment/src/main/java/Enrichment.java)
  - the sample is in the `enrichUsingAsyncService()` method
  - prepare a mock data server: a gRPC-based network service
  - use the Event Journal of an `IMap` as a streaming data source
  - enrich the unbounded data stream by making async gRPC calls to the
    mock service
  - print the results on the console
### [Enrich Using Hash Join](enrichment/src/main/java/Enrichment.java)
  - the sample is in the `enrichUsingHashJoin()` method
  - use the Event Journal of an `IMap` as a streaming data source
  - use a directory of files as a batch data source
  - hash-join an unbounded stream with two batch streams in one step
  - print the results on the console

## Configuration

- [XML Configuration](configuration/configure-xml)
- [YAML Configuration](configuration/configure-yaml)
- [Logging Configuration](configuration/configure-logging)
- [Configure Fault Tolerance](fault-tolerance/src/main/java/FaultTolerance.java)
- Enterprise Feature: Configure SSL
  - [XML, Embedded](enterprise/src/main/resources/hazelcast.xml)
  - [XML, Client](enterprise/src/main/resources/hazelcast-client.xml)
  - [Programmatic, Embedded](enterprise/src/main/java/member/ProgrammaticConfiguration.java)
  - [Programmatic, Client](enterprise/src/main/java/client/ProgrammaticConfiguration.java)

## Job Management

- [Suspend/Resume a Job](job-management/src/main/java/JobSuspendResume.java)
- [Restart/Rescale a Job](job-management/src/main/java/JobManualRestart.java)
- [Inspect and Manage Existing Jobs](job-management/src/main/java/JobTracking.java)
- [Idempotently Submit a Job](job-management/src/main/java/ExclusiveJobExecution.java)
  - submit a job with the same name to two Jet members
  - result: only one job running, both clients get a reference to it
- [Live-Update a Running Job's Code](job-management/src/main/java/JobUpdate.java)

## Integration with Hazelcast IMDG
- [IMap as Source and Sink](hazelcast-connectors/src/main/java/MapSourceAndSinks.java)
- [IMap in a Remote IMDG as Source and Sink](hazelcast-connectors/src/main/java/RemoteMapSourceAndSink.java)
- [Projection and Filtering Pushed into the IMap Source](hazelcast-connectors/src/main/java/MapPredicateAndProjection.java)
- [ICache as Source and Sink](hazelcast-connectors/src/main/java/CacheSourceAndSink.java)
- [IList as Source and Sink](hazelcast-connectors/src/main/java/ListSourceAndSink.java)
- [Event Journal of IMap as a Stream Source](event-journal/src/main/java/MapJournalSource.java)
  - variant with [IMap in a remote cluster](event-journal/src/main/java/MapJournalSource.java)
- [Event Journal of ICache as a Stream Source](event-journal/src/main/java/CacheJournalSource.java)
  - variant with [ICache in a remote cluster](event-journal/src/main/java/CacheJournalSource.java)
- [Data Enrichment Using IMap or ReplicatedMap](enrichment/src/main/java/Enrichment.java)

## Integration with Other Systems

- [Kafka Source](kafka/src/main/java/KafkaSource.java)
  - variant with [Avro Serialization](kafka/src/main/java/avro/KafkaAvroSource.java)
- [Hadoop Distributed File System (HDFS) Source and Sink](hadoop/src/main/java/HadoopWordCount.java)
  - variant with [Avro Serialization](hadoop/src/main/java/avro/HadoopAvro.java)
- [JDBC Source](jdbc/src/main/java/JdbcSource.java)
- [JDBC Sink](jdbc/src/main/java/JdbcSink.java)
- [JMS Queue Source and Sink](jms/src/main/java/JmsQueueSample.java)
- [JMS Topic Source and Sink](jms/src/main/java/JmsTopicSample.java)
- [TCP/IP Socket Source](sockets/src/main/java/StreamTextSocket.java)
- [TCP/IP Socket Sink](sockets/src/main/java/WriteTextSocket.java)
- [File Batch Source](file-io/src/main/java/AccessLogAnalyzer.java)
  - use Jet to analyze an HTTP access log file
  - variant with [Avro serialization](file-io/src/main/java/avro/AvroSource.java)
- [File Streaming Source](file-io/src/main/java/AccessLogStreamAnalyzer.java)
  - analyze the data being appended to log files while the Jet job is
    running
- [File Sink](file-io/src/main/java/AccessLogAnalyzer.java)
  - variant with [Avro serialization](file-io/src/main/java/avro/AvroSink.java)

## Custom Sources and Sinks
- [Custom Source](source-builder/src/main/java/HttpSource.java):
  - start an Undertow HTTP server that collects basic JVM stats
  - construct a custom Jet source based on Java 11 HTTP client
  - apply a sliding window
  - compute linear trend of the JVM metric provided by the HTTP server
  - present the results in a live GUI chart
- [Custom Sink](sink-builder/src/main/java/TopicSink.java)
  - construct a custom Hazelcast `ITopic` sink
- [Custom Side Input for Data Enrichment](enrichment/src/main/java/Enrichment.java)
  - look at the `enrichUsingReplicatedMap()` method

## Integration with Frameworks

### Spring Framework
- [Annotation-Based Spring Context](integration/spring/src/main/java/jet/spring/AnnotationBasedConfigurationSample.java)
  - use programmatic Jet configuration in a Spring Application Context
    class
  - annotation-based dependency injection into a Jet Processor
- [XML-Based Spring Context](integration/spring/src/main/java/jet/spring/XmlConfigurationSample.java)
  - configure Jet as a Spring bean in
    [application-context.xml](integration/spring/src/main/resources/application-context.xml)
  - XML-based dependency injection into a Jet Processor
- [XML-Based Dependency Injection into a Jet
  Processor](integration/spring/src/main/java/jet/spring/XmlConfigurationSample.java)
  - [configure Jet](integration/spring/src/main/resources/application-context-with-schema.xml)
  as a Spring bean using Jet's XML Schema for Spring Configuration
  - XML-based dependency injection into a Jet Processor
- [Spring Boot App that Runs a Jet
  Job](integration/spring/src/main/java/jet/spring/SpringBootSample.java)

### [TensorFlow](integration/tensorflow)

Use a pre-trained TensorFlow model to enrich a stream of movie reviews.
The model classifies natural-language text by the sentiment it expresses.
- [Access TensorFlow using the In-Process
 Model](integration/tensorflow/src/main/java/InProcessClassification.java)
- [Access TensorFlow using a Model
 Server](integration/tensorflow/src/main/java/ModelServerClassification.java)

## Integration with Deployment Environments

- Pivotal Cloud Foundry
  - Spring Boot
  [Application]((integration/pcf/src/main/java/Application.java)) that
  starts Hazelcast Jet in the Pivotal Cloud Foundry environment
- Docker
  - [`Makefile`](integration/docker-compose/Makefile) that uses `docker-compose` to deploy Jet into a Docker container
  - [Docker Cloud Stack File](integration/docker-compose/hazelcast.yml)
  that describes Jet to Docker
- Kubernetes
  - [main Kubernetes README](integration/kubernetes/README.md)
  - [README for Helm users](integration/kubernetes/helm/README.md)

## [Core-API Samples](core-api/README.md)

Code samples that show how to use the low-level Core DAG API directly.
See the dedicated [README](core-api/README.md) for more details.

## License

Hazelcast is available under the Apache 2 License. Please see the
[Licensing section](http://docs.hazelcast.org/docs/latest-dev/manual/html-single/index.html#licensing)
for more information.

## Copyright

Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.
