# Hazelcast Jet

![GitHub release](https://img.shields.io/github/release/hazelcast/hazelcast-jet.svg)
[![Join the chat at https://gitter.im/hazelcast/hazelcast-jet](https://badges.gitter.im/hazelcast/hazelcast-jet.svg)](https://gitter.im/hazelcast/hazelcast-jet?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

<img src="https://github.com/hazelcast/hazelcast-jet/raw/master/logo/hazelcast-jet.png" width="100">

----

[Hazelcast Jet] is an open-source, cloud-native, distributed stream
and batch processing engine.

Jet is simple to set up. The nodes you start discover each other and
form a cluster automatically. You can do the same locally, even on the
same machine (your laptop, for example). This is great for quick testing.

With Jet it's easy to build fault-tolerant and elastic data processing
pipelines. Jet keeps processing data without loss even if a node fails,
and you can add more nodes that immediately start sharing the
computation load.

You can embed Jet as a part of your application, it's just a single JAR
without dependencies. You can also deploy it standalone, as a
stream-processing cluster.

Jet also provides a highly available, distributed in-memory data store.
You can cache your reference data and enrich the event stream with it,
store the results of a computation, or even store the input data you're
about to process with Jet.

----

## Start using Jet

Add this to your `pom.xml` to get the latest Jet as your project
dependency:

```xml
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet</artifactId>
    <version>3.2</version>
</dependency>
```

Since Jet is embeddable, this is all you need to start your first Jet
instance! Read on for a quick example of your first Jet program.

### Batch Processing with Jet

Use this code to start an instance of Jet and tell it to perform some
computation:

```java
String path = "books";

JetInstance jet = Jet.newJetInstance();

Pipeline p = Pipeline.create();

p.readFrom(Sources.files(path))
        .flatMap(line -> Traversers.traverseArray(line.toLowerCase().split("\\W+")))
        .filter(word -> !word.isEmpty())
        .groupingKey(word -> word)
        .aggregate(AggregateOperations.counting())
        .writeTo(Sinks.logger());

jet.newJob(p).join();
```

When you run this, point the `path` variable to some directory with text
files in it. Jet will analyze all the files and give you the word
frequency distribution in the log output (for each word it will say how
many times it appears in the files).

The above was an example of processing data at rest (i.e., _batch
processing_). It's conceptually simpler than stream processing so we
used it as our first example.

### Stream Processing with Jet

For stream processing you need a streaming data source. A simple example
is watching a folder of text files for changes and processing each new
appended line. Here's the code you can try out:

```java
String path = "books";

JetInstance jet = Jet.newJetInstance();

Pipeline p = Pipeline.create();

p.readFrom(Sources.fileWatcher(path))
        .withIngestionTimestamps()
        .setLocalParallelism(1)
        .flatMap(line -> Traversers.traverseArray(line.toLowerCase().split("\\W+")))
        .filter(word -> !word.isEmpty())
        .groupingKey(word -> word)
        .window(WindowDefinition.tumbling(1000))
        .aggregate(AggregateOperations.counting())
        .writeTo(Sinks.logger());

jet.newJob(p).join();
```

Before running this make an empty directory and point the `path`
variable to it. While the job is running copy some text files into it
and Jet will process them right away.

## Features:

* Constant low latency - predictable latency is a design goal
* Zero dependencies - single JAR which is embeddable (minimum JDK 8)
* Cloud Native - with [Docker images](https://hub.docker.com/r/hazelcast/hazelcast-jet/)
and [Kubernetes support](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/master/integration/kubernetes)
including Helm Charts.
* Elastic - Jet can scale jobs up and down while running
* Fault Tolerant - At-least-once and exactly-once processing guarantees
* In-memory storage - Jet provides robust distributed in-memory storage
for caching, enrichment or storing job results
* Sources and sinks for Apache Kafka, Hadoop, Hazelcast IMDG, sockets, files
* Dynamic node discovery for both on-premise and cloud deployments.

## Distribution

You can download the distribution package which includes command-line
tools from [jet.hazelcast.org](http://jet.hazelcast.org/download/).

## Documentation

See the [Hazelcast Jet Reference Manual].

## Code Samples

See [examples folder](examples) for some examples.

## Connectors

| Name                                                         | Description                                                  |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| [Amazon S3](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/s3/src/main/java/com/hazelcast/jet/s3) | A connector that allows AWS S3 read/write support for Hazelcast Jet. |
| [Apache Avro](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/avro)   | Source and sink connector for Avro files.                                                     |
| [Apache Hadoop](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/hadoop/src/main/java/com/hazelcast/jet/hadoop) | A connector that allows Apache Hadoop read/write support for Hazelcast Jet. |
| [Apache Kafka](https://github.com/hazelcast/hazelcast-jet/tree/master/extensions/kafka) | A connector that allows consuming/producing events from/to Apache Kafka. |
| [Debezium](https://github.com/hazelcast/hazelcast-jet-contrib/blob/master/debezium) | A Hazelcast Jet connector for Debezium which enables Hazelcast Jet pipelines to consume CDC events from various databases. |
| [Elasticsearch](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/elasticsearch) | A Hazelcast Jet connector for Elasticsearch for querying/indexing objects from/to Elasticsearch. |
| [Files](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java)            | Connector for local filesystem.                                               |
| [Hazelcast Cache Journal](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java) | Connector for change events on caches in local and remote Hazelcast clusters. |
| [Hazelcast Cache](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java)         | Connector for caches in local and remote Hazelcast clusters.                  |
| [Hazelcast List](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java)          | Connector for lists in local and remote Hazelcast clusters.                   |
| [Hazelcast Map Journal](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java)   | Connector for change events on maps in local and remote Hazelcast clusters.   |
| [Hazelcast Map](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java)           | Connector for maps in local and remote Hazelcast clusters.                    |
| [InfluxDb](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/influxdb) | A Hazelcast Jet Connector for InfluxDb which enables pipelines to read/write data points from/to InfluxDb. |
| [JDBC](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java)          | Connector for relational databases via JDBC.                                  |
| [JMS](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java)           | Connector for JMS topics and queues.|
| [Kafka Connect](https://github.com/hazelcast/hazelcast-jet-contrib/blob/master/kafka-connect) | A generic Kafka Connect source provides ability to plug any Kafka Connect source for data ingestion to Jet pipelines.|
| [MongoDB](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/mongodb) | A Hazelcast Jet connector for MongoDB for querying/inserting objects from/to MongoDB. |
| [Redis](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/redis) | Hazelcast Jet connectors for various Redis data structures.  |
| [Socket](https://github.com/hazelcast/hazelcast-jet/blob/master/hazelcast-jet-core/src/main/java/com/hazelcast/jet/pipeline/Sources.java)        | Connector for TCP sockets.                                                    |
| [Twitter](https://github.com/hazelcast/hazelcast-jet-contrib/blob/master/twitter)  | A Hazelcast Jet connector for consuming data from Twitter stream sources in Jet pipelines. |

See [hazelcast-jet-contrib](https://github.com/hazelcast/hazelcast-jet-contrib) repository for more detailed information on community supported connectors and tools. 

## Architecture

See the [architecture](https://jet.hazelcast.org/architecture/) and
[performance](https://jet.hazelcast.org/performance/) pages for
more details about Jet's internals and design.

## Start Developing Hazelcast Jet

### Use Latest Snapshot Release

You can always use the latest snapshot release if you want to try the features
currently under development.

Maven snippet:

```xml
<repositories>
    <repository>
        <id>snapshot-repository</id>
        <name>Maven2 Snapshot Repository</name>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
        <snapshots>
            <enabled>true</enabled>
            <updatePolicy>daily</updatePolicy>
        </snapshots>
    </repository>
</repositories>
<dependencies>
    <dependency>
        <groupId>com.hazelcast.jet</groupId>
        <artifactId>hazelcast-jet</artifactId>
        <version>4.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

### Build From Source

#### Requirements

* JDK 8 or later

To build on Linux/MacOS X use:
```
./mvnw clean package -DskipTests
```
for Windows use:
```
mvnw clean package -DskipTests
```

### Contributions

We encourage pull requests and process them promptly.

To contribute:

* Complete the [Hazelcast Contributor Agreement](https://hazelcast.atlassian.net/wiki/display/COM/Hazelcast+Contributor+Agreement)
* If you're not familiar with Git, see the [Hazelcast Guide for Git](https://hazelcast.atlassian.net/wiki/display/COM/Developing+with+Git) for our Git process

### Community

Hazelcast Jet team actively answers questions on [Stack Overflow](https://stackoverflow.com/tags/hazelcast-jet).

You are also encouraged to join the [hazelcast-jet mailing list](http://groups.google.com/group/hazelcast-jet)
if you are interested in community discussions

## License
          
Source code in this repository is covered by one of two licenses:   

 1. [Apache License 2.0](licenses/apache-v2-license.txt)   
 2. [Hazelcast Community License](licenses/hazelcast-community-license.txt).   

The default license throughout the repository is Apache License 2.0 unless the  
header specifies another license. Please see the [Licensing section](https://docs.hazelcast.org/docs/jet/latest-dev/manual/#licensing) for more information.

## Copyright

Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.


[Hazelcast Jet]: http://jet.hazelcast.org
[Hazelcast Jet Reference Manual]: https://docs.hazelcast.org/docs/jet/latest/manual/
[Hazelcast Jet Code Samples]: https://github.com/hazelcast/hazelcast-jet-code-samples
