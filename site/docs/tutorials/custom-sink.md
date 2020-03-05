---
title: Custom Sinks
description: Tutorial on how to define custom sinks.
---

In the [Custom Sources and Sinks](../api/sources-sinks.md#custom-sources-and-sinks)
section of our [Sources and Sinks](../api/sources-sinks.md) programming
guide we have seen some basic examples of user-defined sources and
sinks. Let us now examine more examples which cover some of the
trickier aspects of writing our own sinks.

Let's write a sink that functions like a **file logger**. You set it up
with a filename and it will write one line for each input it gets into
that file. The lines will be composed of a **timestamp** and then the
**`toString()`** form of whatever input object produced the line.
Here's a sample:

```text
1583309377078,SimpleEvent(timestamp=10:09:37.000, sequence=2900)
1583309377177,SimpleEvent(timestamp=10:09:37.100, sequence=2901)
1583309377277,SimpleEvent(timestamp=10:09:37.200, sequence=2902)
1583309377376,SimpleEvent(timestamp=10:09:37.300, sequence=2903)
```

## 1. Start Hazelcast Jet

1. [Download](https://github.com/hazelcast/hazelcast-jet/releases/download/v4.0/hazelcast-jet-4.0.zip)
  Hazelcast Jet

2. Unzip it:

```bash
cd <where_you_downloaded_it>
unzip hazelcast-jet-4.0.zip
cd hazelcast-jet-4.0
```

If you already have Jet and you skipped the above steps, make sure to
follow from here on.

3. Start Jet:

```bash
bin/jet-start
```

4. When you see output like this, Hazelcast Jet is up:

```text
Members {size:1, ver:1} [
    Member [192.168.1.5]:5701 - e7c26f7c-df9e-4994-a41d-203a1c63480e this
]
```

From now on we assume Hazelcast Jet is running on your machine.

## 2. Create a New Java Project

We'll assume you're using an IDE. Create a blank Java project named
`custom-sink-tutorial` and copy the Gradle or Maven file
into it:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
plugins {
    id 'java'
}

group 'org.example'
version '1.0-SNAPSHOT'

repositories.mavenCentral()

dependencies {
    compile 'com.hazelcast.jet:hazelcast-jet:4.0'
}

jar.manifest.attributes 'Main-Class': 'org.example.LogProducer'
```

<!--Maven-->

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>custom-sink-tutorial</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.target>1.8</maven.compiler.target>
        <maven.compiler.source>1.8</maven.compiler.source>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.hazelcast.jet</groupId>
            <artifactId>hazelcast-jet</artifactId>
            <version>4.0</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <configuration>
                    <archive>
                        <manifest>
                            <mainClass>org.example.LogProducer</mainClass>
                        </manifest>
                    </archive>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

<!--END_DOCUSAURUS_CODE_TABS-->

## 3. Define Sink

Let us now write our actual sink.

```java
package org.example;

import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;

import java.io.PrintWriter;

class Sinks {

    static Sink<Object> buildLogSink() {
        return SinkBuilder.sinkBuilder(
                "cpu-sink", pctx -> new PrintWriter("data." + pctx.globalProcessorIndex() + ".csv"))
                .receiveFn((writer, item) -> {
                    writer.println(String.format("%d,%s", System.currentTimeMillis(), item.toString()));
                    writer.flush();
                })
                .destroyFn(writer -> writer.close())
                .build();
    }

}
```

## 4. Define Jet Job

The next thing we need to do is to write the Jet code that creates a
pipeline and the job to be submitted for execution:

```java
package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.test.TestSources;

public class LogProducer {

    public static void main(String[] args) {
        Sink<Object> cpuSink = Sinks.buildLogSink();

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(10))
                .withoutTimestamps()
                .writeTo(cpuSink);

        JobConfig cfg = new JobConfig().setName("log-producer");
        Jet.bootstrappedInstance().newJob(p, cfg);
    }

}
```

## 5. Package

Now that we have all the pieces, we need to submit it to Jet for
execution. Since Jet runs on our machine as a standalone cluster in a
standalone process we need to give it all the code that we have written.

For this reason we create a fat jar containing everything we need. All
we need to do is to run the build command:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
gradle build
```

This will produce a jar file called `custom-sink-tutorial-1.0-SNAPSHOT.jar`
in the `build/libs` folder of our project.

<!--Maven-->

```bash
mvn package
```

This will produce a jar file called `custom-sink-tutorial-1.0-SNAPSHOT.jar`
in the `target` folder or our project.

<!--END_DOCUSAURUS_CODE_TABS-->

## 6. Submit for Execution

Assuming our cluster is [still running](#1-start-hazelcast-jet) all we
need to issue is following command:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
<path_to_jet>/bin/jet submit build/libs/custom-sink-tutorial-1.0-SNAPSHOT.jar
```

<!--Maven-->

```bash
<path_to_jet>/bin/jet submit target/custom-sink-tutorial-1.0-SNAPSHOT.jar
```

<!--END_DOCUSAURUS_CODE_TABS-->

In the log of the Jet member we should see a message like this:

```text
...
Start executing job 'log-producer', execution 03fd-63b4-4700-0001, execution graph in DOT format:
digraph DAG {
    "itemStream" [localParallelism=1];
    "cpu-sink" [localParallelism=1];
    "itemStream" -> "cpu-sink" [queueSize=1024];
}
...
```

In the folder where the Jet member was started a new file should show
up, called `data.0.csv`, containing lines like (at getting more and
more each second):

```text
...
1583309377078,SimpleEvent(timestamp=10:09:37.000, sequence=2900)
1583309377177,SimpleEvent(timestamp=10:09:37.100, sequence=2901)
1583309377277,SimpleEvent(timestamp=10:09:37.200, sequence=2902)
1583309377376,SimpleEvent(timestamp=10:09:37.300, sequence=2903)
...
```

Our sink works! Now let's make it better.

## 8. Add Batching

Our sink uses a `PrintWriter` which has internal buffering we could use
to make it more efficient. Jet allows us to make buffering a first-class
concern and deal with it explicitly by taking an optional `flushFn`
which it will call at regular intervals.

To apply this to our example we need to update our sink definition like
this:

```java
package org.example;

import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;

import java.io.PrintWriter;

class Sinks {

    static Sink<Object> buildLogSink() {
        return SinkBuilder.sinkBuilder(
                "cpu-sink", pctx -> new PrintWriter("data." + pctx.globalProcessorIndex() + ".csv"))
                .receiveFn((writer, item) -> {
                    writer.println(String.format("%d,%s", System.currentTimeMillis(), item.toString()));
                })
                .flushFn(writer -> writer.flush())
                .destroyFn(writer -> writer.close())
                .build();
    }

}
```

These changes will not produce visible effects in the behaviour of our
sink, but they will make it much more efficient. Benchmarking that
however is a bit beyond the scope of this tutorial.

## 9. Increase Parallelism

Jet builds the sink to be distributed by default: each member of the Jet
cluster has a processor running it. You can configure how many parallel
processors there are on each member (the **local parallelism**) by
calling `SinkBuilder.preferredLocalParallelism()`. By default there will
be one processor per member.

The overall job output consists of the contents of all the files
written by all processor instances put together.

Let's increase the local parallelism from the default value of 1 to 2:

```java
package org.example;

import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;

import java.io.PrintWriter;

class Sinks {

    static Sink<Object> buildLogSink() {
        return SinkBuilder.sinkBuilder(
                "cpu-sink", pctx -> new PrintWriter("data." + pctx.globalProcessorIndex() + ".csv"))
                .receiveFn((writer, item) -> {
                    writer.println(String.format("%d,%s", System.currentTimeMillis(), item.toString()));
                })
                .flushFn(writer -> writer.flush())
                .destroyFn(writer -> writer.close())
                .preferredLocalParallelism(2)
                .build();
    }

}
```

Now let's [repackage](#5-package) our updated code just as before.

Before we [re-submit it for execution](#6-submit-for-execution), just
like before we might want to cancel the previous job:

```bash
<path_to_jet>/bin/jet cancel log-producer
```

The behavioral change we can notice now is that there will be two output
files, `data.0.csv` and `data.1.csv`, each containing half of the output
data.

> Note: we could add a second member to the Jet cluster now. At that
> point we would have two members, both with local parallelism of 2.
> There would be 4 output files. You would notice however that all
> the data is in the files written by the processors of a single Jet
> member.
>
> The other members don't get any data, because on one hand our pipeline
> doesn't contain any operation that would generate distributed edges
> (ones that carry data from one member to another) and on the other
> hand the test source we have used only creates one instance globally,
> regardless of the number of members we have in the cluster. The member
> containing the test source instance will process all the data in this
> case. Real sources don't usually have this limitation.

## 10. Make it Fault Tolerant

Sinks built via `SinkBuilder` don’t participate in the fault tolerance
protocol. You can’t preserve any internal state if a job fails and gets
restarted. In a job with snapshotting enabled your sink will still
receive every item at least once. If you ensure that after the `flushFn`
is called all the previous items are persistently stored, your sink
provides an at-least-once guarantee. If you don't (like our first
example without the flushFn), your sink can also miss items. If the
system you’re storing the data into is idempotent (i.e. writing the same
thing multiple times has the exact same effect as writing it a single
time - obviously not the case with our example), then your sink will
have an exactly-once guarantee.
