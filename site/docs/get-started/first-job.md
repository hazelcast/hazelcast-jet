---
title: Write and run your first job
id: first-job
---

## Requirements

Hazelcast Jet is distributed as a single JAR with no other dependencies. 
It requires Java version 8 or higher to run.

### Install as a Java depedency

The easiest way to get started with Hazelcast Jet is to add it as a
dependency to a Java application. Jet is packaged as just a single Jet
JAR with no other dependencies that contains everything you need to get
started.

Below are the Maven and Gradle snippets you can use:

<!--DOCUSAURUS_CODE_TABS-->
<!--Maven-->
```xml
<dependencies>
  <dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet</artifactId>
    <version>3.2.2</version>
  </dependency>
</dependencies>
```
<!--Gradle-->
```
compile 'com.hazelcast.jet:hazelcast-jet:3.2.2'
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Write your pipeline

In order to run a Jet job, you must first define a data pipeline which
defines what data will be processed. To create a data pipeline, you need
a data source and a data sink. The easiest way to get started is to use
the provided the test sources within Jet. These sources emit a mock
stream at a fixed rate and can be used for writing test pipelines.

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.itemStream(10))
 .withIngestionTimestamps()
 .filter(event -> event.sequence() % 2 == 0)
 .setName("filter out odd numbers")
 .writeTo(Sinks.logger());
```

Each test item emitted has a _sequence_ and a _timestamp_. In this case,
we're only interested in the event numbers and we will filter the odd
numbers out.

### Create an embedded Jet node, and run the pipeline

Now that we have defined our pipeline, we need to create a Jet node to
submit the pipeline to. An embedded is a fully functional Jet node, running
inside the same JVM and has the same performance as a standalone node.

You can create a single Jet node and submit the job using the following syntax:

```java
JetInstance jet = Jet.newJetInstance();
Job job = jet.newJob(p).join();
```

Note that submitting a job is asnychronous, so we must also call `join()`
afterwards to make sure that we wait for the job to proceed. When you the
application, you should output like the following, indicating that the
odd numbers have been filtering out:

```log
11:28:24.039 [   INFO] [             loggerSink#0] (timestamp=11:28:24.000, sequence=0)
11:28:24.246 [   INFO] [             loggerSink#0] (timestamp=11:28:24.200, sequence=2)
11:28:24.443 [   INFO] [             loggerSink#0] (timestamp=11:28:24.400, sequence=4)
11:28:24.647 [   INFO] [             loggerSink#0] (timestamp=11:28:24.600, sequence=6)
11:28:24.846 [   INFO] [             loggerSink#0] (timestamp=11:28:24.800, sequence=8)
11:28:25.038 [   INFO] [             loggerSink#0] (timestamp=11:28:25.000, sequence=10)
11:28:25.241 [   INFO] [             loggerSink#0] (timestamp=11:28:25.200, sequence=12)
11:28:25.443 [   INFO] [             loggerSink#0] (timestamp=11:28:25.400, sequence=14)
11:28:25.643 [   INFO] [             loggerSink#0] (timestamp=11:28:25.600, sequence=16)
```
