---
title: Write and run your first job
id: first-job
---

Let's write some data processing code and have Jet run it for us.

### Start a Java Project

By now you should have some version of Java installed (at least 8) and
your build tool of preference, Maven or Gradle.

The first step is to add the Jet JAR to your build:

<!--DOCUSAURUS_CODE_TABS-->
<!--Maven-->
```xml
<dependencies>
  <dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet</artifactId>
    <version>4.0</version>
  </dependency>
</dependencies>
```
<!--Gradle-->
```
compile 'com.hazelcast.jet:hazelcast-jet:4.0'
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Write Your Data Pipeline

Unlike some other paradigms you may have met, in the world of
distributed stream processing we specify not just the processing steps
but also where to pull the data from and where to deliver the results.
This means that as soon as you deploy your code, it takes action and
starts moving the data through the pipeline.

With this in mind let's start writing code. Instead of connecting to
actual systems we'll start simple, using generated data as the source
and your screen as the sink. This works well with a locally started Jet
instance:

```java
public static void main(String[] args) {
  Pipeline p = Pipeline.create();
  p.readFrom(TestSources.itemStream(10))
   .withIngestionTimestamps()
   .filter(event -> event.sequence() % 2 == 0)
   .setName("filter out odd numbers")
   .writeTo(Sinks.logger());
}
```

`itemStream()` emits `SimpleEvent`s that have a _sequence_ and a
_timestamp_. In this case we're only interested in the sequence numbers
and we keep only the even-numbered events.

### Start Embedded Jet and Run the Pipeline

To create a single Jet node and submit the job to it, add this code to
the bottom of the `main` method:

```java
JetInstance jet = Jet.newJetInstance();
Job job = jet.newJob(p).join();
```

It will start a full-featured Jet node right there in the JVM where you
call it and submit your pipeline to it. If you were submitting the code
to an external Jet cluster, the syntax would be the same because
`JetInstance` can represent both an embedded instance or a remote one
via a local proxy object. You'd just call a different method to create
the client instance.

Once you submit a job, it has a life of its own. It is not coupled to
the client that submitted it, so the client can disconnect without
affecting the job. In our simple code we call `job.join()` so we keep
the JVM alive while the job lasts.

The output should look like this:

```log
11:28:24.039 [INFO] [loggerSink#0] (timestamp=11:28:24.000, sequence=0)
11:28:24.246 [INFO] [loggerSink#0] (timestamp=11:28:24.200, sequence=2)
11:28:24.443 [INFO] [loggerSink#0] (timestamp=11:28:24.400, sequence=4)
11:28:24.647 [INFO] [loggerSink#0] (timestamp=11:28:24.600, sequence=6)
11:28:24.846 [INFO] [loggerSink#0] (timestamp=11:28:24.800, sequence=8)
11:28:25.038 [INFO] [loggerSink#0] (timestamp=11:28:25.000, sequence=10)
11:28:25.241 [INFO] [loggerSink#0] (timestamp=11:28:25.200, sequence=12)
11:28:25.443 [INFO] [loggerSink#0] (timestamp=11:28:25.400, sequence=14)
11:28:25.643 [INFO] [loggerSink#0] (timestamp=11:28:25.600, sequence=16)
```
