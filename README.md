# Hazelcast Jet

---

[![Join the chat at https://gitter.im/hazelcast/hazelcast-jet](https://badges.gitter.im/hazelcast/hazelcast-jet.svg)](https://gitter.im/hazelcast/hazelcast-jet?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

<img src="https://github.com/hazelcast/hazelcast-jet/raw/master/logo/hazelcast-jet.png" width="100">

---

[Hazelcast Jet] is an open source, cloud native, distributed stream 
and batch processing engine.

It's simple to set up, embeddable, has no other dependencies and makes it easy to 
build fault-tolerant, elastic data processing pipelines. It additionally provides 
robust, distributed in-memory storage for caching, enrichment and storing
processing results.

---

## Start using Jet

Use the following Maven snippet to start using the latest version of Jet:

```xml
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet</artifactId>
    <version>3.0</version>
</dependency>
```

### Batch Processing with Jet

Once you have your project ready, you can use the following snippet
to create a Jet node and start processing data. You'll 
need a folder with some text files in it.

```java
JetInstance jet = Jet.newJetInstance();

Pipeline p = Pipeline.create();

p.drawFrom(Sources.files(path))
        .flatMap(line -> Traversers.traverseArray(line.toLowerCase().split("\\W+")))
        .filter(word -> !word.isEmpty())
        .groupingKey(word -> word)
        .aggregate(AggregateOperations.counting())
        .drainTo(Sinks.logger());

jet.newJob(p).join();
```

### Stream Processing with Jet

For stream processing, you'll need a streaming data source. The simplest
one we can use is to watch over a folder. Create an empty folder,
run your program and then start adding some files to the folder. 

```java
JetInstance jet = Jet.newJetInstance();

Pipeline p = Pipeline.create();

p.drawFrom(Sources.fileWatcher(path))
        .withIngestionTimestamps()
        .setLocalParallelism(1)
        .flatMap(line -> Traversers.traverseArray(line.toLowerCase().split("\\W+")))
        .filter(word -> !word.isEmpty())
        .groupingKey(word -> word)
        .window(WindowDefinition.tumbling(1000))
        .aggregate(AggregateOperations.counting())
        .drainTo(Sinks.logger());

jet.newJob(p).join();
```

## Features:

* Constant low-latency - predictable latency is a design goal
* Zero dependencies - single JAR which is embeddable (minimum JDK 8)
* Elastic - Jet can scale jobs up and down while running
* Fault Tolerant - At-least-once and exactly-once processing guarantees
* In memory storage - Jet provides robust distributed in memory storage 
for caching, enrichment or storing job results
* Sources and sinks for Apache Kafka, Hadoop, Hazelcast IMDG, sockets, files
* Dynamic node discovery for both on-premise and cloud deployments.
* Cloud Native - with [Docker images](https://hub.docker.com/r/hazelcast/hazelcast-jet/) 
and [Kubernetes support](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/master/integration/kubernetes)
including Helm Charts.

## Distribution

You can also download the distribution package which includes command line tools
from [jet.hazelcast.org](http://jet.hazelcast.org/download/).

## Documentation 

See the [Hazelcast Jet Reference Manual].

## Code Samples

See [Hazelcast Jet Code Samples] for some examples.

## Additional Connectors

See [hazelcast-jet-contrib](github.com/hazelcast/hazelcast-jet-contrib) repository for community supported 
connectors and tools.

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
        <version>3.1-SNAPSHOT</version>
    </dependency>
</dependencies>
```

### Build From Source

#### Requirements

* JDK 8 or later
* [Apache Maven](https://maven.apache.org/) version 3.5.2 or later

To build, use the command:

```
mvn clean package -DskipTests
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

Hazelcast Jet is available under the Apache 2 License. Please see the
[Licensing section](http://docs.hazelcast.org/docs/latest-dev/manual/html-single/index.html#licensing) for more information.

## Copyright

Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.


[Hazelcast Jet]: http://jet.hazelcast.org
[Hazelcast Jet Reference Manual]: https://docs.hazelcast.org/docs/jet/latest/manual/
[Hazelcast Jet Code Samples]: https://github.com/hazelcast/hazelcast-jet-code-samples
