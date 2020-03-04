---
title: Custom Sources
description: Tutorial on how to define custom sources.
---

In the [Custom Sources and Sinks](../api/sources-sinks.md#custom-sources-and-sinks)
section of our [Sources and Sinks](../api/sources-sinks.md) programming
guide we have seen some basic examples of user-defined sources and
sinks. Let us now examine more examples which cover some of the
trickier aspects of writing our own sources.

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
`custom-source-tutorial` and copy the Gradle or Maven file
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
```

<!--Maven-->

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>custom-source-tutorial</artifactId>
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
</project>
```

<!--END_DOCUSAURUS_CODE_TABS-->

## 3. Define Source

Let's write a source which is capable of reading lines of text from
a file. We will start with a simple, batch version:

```java
package org.example;

import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;

import java.io.BufferedReader;
import java.io.FileReader;

public class Sources {

    static BatchSource<String> buildLinesSource() {
        return SourceBuilder
                .batch("cpu-source", x -> new BufferedReader(
                                                new FileReader("lines.txt")))
                .<String>fillBufferFn((in, buf) -> {
                    String line = in.readLine();
                    if (line != null) {
                        buf.add(line);
                    } else {
                        buf.close();
                    }
                })
                .destroyFn(buf -> buf.close())
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
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;

public class LinesConsumer {

    public static void main(String[] args) {
        BatchSource<String> cpuSource = Sources.buildLinesSource();

        Pipeline p = Pipeline.create();
        p.readFrom(cpuSource)
                .writeTo(com.hazelcast.jet.pipeline.Sinks.logger());

        JobConfig cfg = new JobConfig().setName("lines-consumer");
        Jet.bootstrappedInstance().newJob(p, cfg);
    }

}
```

## 5. Package

Now that we have all the pieces, we need to submit it to Jet for
execution. Since Jet runs on our machine as a standalone cluster in a
standalone process we need to give it all the code that we have written.

For this reason we create a jar containing everything we need. All we
need to do is to run the build command:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
gradle build
```

This will produce a jar file called `custom-source-tutorial-1.0-SNAPSHOT.jar`
in the `build/libs` folder of our project.

<!--Maven-->

```bash
mvn package
```

This will produce a jar file called `custom-source-tutorial-1.0-SNAPSHOT.jar`
in the `target` folder or our project.

<!--END_DOCUSAURUS_CODE_TABS-->

We should check that it contains the `org.example` classes we have
written.

## 6. Submit for Execution

Assuming our cluster is [still running](#1-start-hazelcast-jet) all we
need to issue is following command:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
<path_to_jet>/bin/jet submit \
    --class org.example.LinesConsumer \
    build/libs/custom-source-tutorial-1.0-SNAPSHOT.jar
```

<!--Maven-->

```bash
<path_to_jet>/bin/jet submit \
    --class org.example.LinesConsumer \
    target/custom-source-tutorial-1.0-SNAPSHOT.jar
```

<!--END_DOCUSAURUS_CODE_TABS-->

In the log of the Jet member we should see a message like this:

```text
...
Start executing job 'lines-consumer', execution 03fd-a4ec-bd40-0001, execution graph in DOT format:
digraph DAG {
    "cpu-source" [localParallelism=1];
    "loggerSink" [localParallelism=1];
    "cpu-source" -> "loggerSink" [queueSize=1024];
}
...
```

> Note: we are assuming that we have a text file called `lines.txt` with
> lines in it present in the Jet members working directory.

The output you get should contain logged versions of the lines from
your `lines.txt` file. We had this (output produced by the custom sink
we have defined in [another tutorial](custom-sink.md)):

```text
... 1583315484503,0.073239,SimpleEvent(timestamp=11:51:24.500, sequence=52134)
... 1583315484703,0.090153,SimpleEvent(timestamp=11:51:24.700, sequence=52136)
... 1583315484903,0.088542,SimpleEvent(timestamp=11:51:24.900, sequence=52138)
```

## 7. Add Batching

Our source works, but it's not efficient, because it always just
retrieves one line at a time. Optimally the `fillBufferFn` should fill
the buffer with all the items it can acquire without blocking. As a
rule of thumb 100 items at a time is enough to dwarf any per-call
overheads within Jet.

The function may block as well, if need be, but taking longer than a
second to complete can have negative effects on the overall performance
of the processing pipeline.

To make it more efficient we could change our `fillBufferFn` like this:

```java
package org.example;

import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;

import java.io.BufferedReader;
import java.io.FileReader;

public class Sources {

    static BatchSource<String> buildLinesSource() {
        return SourceBuilder
                .batch("cpu-source", x -> new BufferedReader(
                                                        new FileReader("lines.txt")))
                .<String>fillBufferFn((in, buf) -> {
                    for (int i = 0; i < 128; i++) {
                        String line = in.readLine();
                        if (line == null) {
                            buf.close();
                            return;
                        }
                        buf.add(line);
                    }
                })
                .destroyFn(buf -> buf.close())
                .build();
    }

}
```

Like with the sink, these changes will not produce visible effects in
the behaviour of our source, but they will make it much more efficient.
Benchmarking that however is a bit beyond the scope of this tutorial.

## 8. Make it Unbounded

Custom sources don't need to batching ones. They can also provide
*unbounded* data. Let's see an example of such a `StreamSource`, one
that reads lines from the network:

```java
package org.example;

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class Sources {

    //...

    static StreamSource<String> buildNetworkSource() {
        return SourceBuilder
                .stream("http-source", ctx -> {
                    int port = 11000;
                    ServerSocket serverSocket = new ServerSocket(port);
                    ctx.logger().info(String.format("Waiting for connection on port %d ...", port));
                    Socket socket = serverSocket.accept();
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));
                    ctx.logger().info(String.format("Data source connected on port %d.", port));
                    return reader;
                })
                .<String>fillBufferFn((reader, buf) -> {
                    for (int i = 0; i < 128; i++) {
                        if (!reader.ready()) {
                            return;
                        }
                        String line = reader.readLine();
                        if (line == null) {
                            buf.close();
                            return;
                        }
                        buf.add(line);
                    }
                })
                .destroyFn(reader -> reader.close())
                .build();
    }

}
```

The Jet code needed for it will be slightly different (because it's a
stream source, not a batch one):

```java
package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;

public class NetworkConsumer {

    public static void main(String[] args) {
        StreamSource<String> cpuSource = Sources.buildNetworkSource();

        Pipeline p = Pipeline.create();
        p.readFrom(cpuSource)
                .withoutTimestamps()
                .peek()
                .writeTo(com.hazelcast.jet.pipeline.Sinks.noop());

        JobConfig cfg = new JobConfig().setName("network-consumer");
        Jet.bootstrappedInstance().newJob(p, cfg);
    }

}
```

Besides that, working with it happens just like with our previous
source: [package](#5-package) and [submit](#6-submit-for-execution),
just don't forget to use `NetworkConsumer` as the main class.

When it starts it should print following and wait for an incoming
network connection:

```text
[jet] [4.1-SNAPSHOT] Waiting for connection on port 11000 ...
```

You can then just go ahead and send it some lines:

```bash
cat linex.txt | nc 127.0.0.1 11000
```

The output should look the same way as before:

```text
... Output to ordinal 0: 1583310272413,0.185707,SimpleEvent(timestamp=10:24:32.400, sequence=14)
... Output to ordinal 0: 1583310272613,1.000000,SimpleEvent(timestamp=10:24:32.600, sequence=16)
... Output to ordinal 0: 1583310272813,0.052980,SimpleEvent(timestamp=10:24:32.800, sequence=18)
```

## 9. Add Timestamps

In the Jet code we wrote for this network source we can notice that
there is an extra line, which wasn't there when we used a batch source
(`withoutTimestamps()`). It is needed because for stream sources Jet
has to know what kind of event timestamps they will provide (if any). Now
we are using it without timestamps, but this unfortunately means that
we aren't allowed to use [Windowed Aggregation](windowing.md) in
our pipeline.

There are multiple ways to fix this (we can add timestamps in the
pipeline after the source), but the most convenient one is to provide
the timestamps right in the source.

Let's assume the data that will come in over the network is always in
the same as we've used so far. In that case we know that each
line starts with a timestamp, so we could modify our source like this:

```java
package org.example;

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class Sources {

    //...

    static StreamSource<String> buildNetworkSource() {
        return SourceBuilder
                .timestampedStream("http-source", ctx -> {
                    int port = 11000;
                    ServerSocket serverSocket = new ServerSocket(port);
                    ctx.logger().info(String.format("Waiting for connection on port %d ...", port));
                    Socket socket = serverSocket.accept();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    ctx.logger().info(String.format("Data source connected on port %d.", port));
                    return reader;
                })
                .<String>fillBufferFn((reader, buf) -> {
                    for (int i = 0; i < 128; i++) {
                        if (!reader.ready()) {
                            return;
                        }

                        String line = reader.readLine();
                        if (line == null) {
                            buf.close();
                            return;
                        }

                        buf.add(line, Long.parseLong(line.substring(0, line.indexOf(','))));
                    }
                })
                .destroyFn(reader -> reader.close())
                .build();
    }

}
```

The Jet code changes too:

```java
package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;

public class NetworkConsumer {

    public static void main(String[] args) {
        StreamSource<String> cpuSource = Sources.buildNetworkSource();

        Pipeline p = Pipeline.create();
        p.readFrom(cpuSource)
                .withNativeTimestamps(0)
                .peek()
                .writeTo(com.hazelcast.jet.pipeline.Sinks.noop());

        JobConfig cfg = new JobConfig().setName("network-consumer");
        Jet.bootstrappedInstance().newJob(p, cfg);
    }

}
```

The output will be very similar but with a crucial difference, now we
have timestamps:

```text
... Output to ordinal 0: 1583310272413,0.185707,SimpleEvent(
         timestamp=10:24:32.400, sequence=14) (eventTime=10:24:32.413)
... Output to ordinal 0: 1583310272613,1.000000,SimpleEvent(
         timestamp=10:24:32.600, sequence=16) (eventTime=10:24:32.613)
... Output to ordinal 0: 1583310272813,0.052980,SimpleEvent(
         timestamp=10:24:32.800, sequence=18) (eventTime=10:24:32.813)
```

## 10. Increase Parallelism

In the examples we showed so far the source was non-distributed: Jet
will create just a single processor in the whole cluster to serve all
the data. This is an easy and obvious way to create a source connector.

If you want to create a distributed source, the challenge is
coordinating all the parallel instances to appear as a single, unified
source.

In our somewhat contrived example we could simply make each instance
listen on its own separate port. We can achieve this by modifying the
`createFn` and making use of the unique, global processor index
available in the `Processor.Context` object we get handed there:

```java
package org.example;

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class Sources {

    //...

    static StreamSource<String> buildNetworkSource() {
        return SourceBuilder
                .timestampedStream("http-source", ctx -> {
                    int port = 11000 + ctx.globalProcessorIndex();
                    ServerSocket serverSocket = new ServerSocket(port);
                    ctx.logger().info(String.format("Waiting for connection on port %d ...", port));
                    Socket socket = serverSocket.accept();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    ctx.logger().info(String.format("Data source connected on port %d.", port));
                    return reader;
                })
                .<String>fillBufferFn((reader, buf) -> {
                    for (int i = 0; i < 128; i++) {
                        if (!reader.ready()) {
                            return;
                        }

                        String line = reader.readLine();
                        if (line == null) {
                            buf.close();
                            return;
                        }

                        buf.add(line, Long.parseLong(line.substring(0, line.indexOf(','))));
                    }
                })
                .destroyFn(reader -> reader.close())
                .distributed(2)
                .build();
        }

}
```

Notice that we have added an extra call to specify the local parallelism
of the source (the `distributed()` method). This means that each Jet
cluster member will now create two such sources.

If we submit this now for execution, we will see following lines:

```text
[jet] [4.1-SNAPSHOT] Waiting for connection on port 11000 ...
[jet] [4.1-SNAPSHOT] Waiting for connection on port 11001 ...
```

## 11. Add Fault Tolerance

If you want your source to behave correctly within a streaming Jet job
that has a processing guarantee configured (**at-least-once** or
**exactly-once**), you must help Jet with saving the operational state
of your context object to the snapshot storage.

There are two functions you must supply:

* **`createSnapshotFn`** returns a serializable object that has all the
  data you’ll need to restore the operational state
* **`restoreSnapshotFn`** applies the previously saved snapshot to the
  current context object

While a job is running, Jet calls `createSnapshotFn` at regular
intervals to save the current state.

When Jet resumes a job, it will:

* create your context object the usual way, by calling `createFn`
* retrieve the latest snapshot object from its storage
* pass the context and snapshot objects to `restoreSnapshotFn`
* start calling `fillBufferFn`, which must start by emitting the same
  item it was about to emit when createSnapshotFn was called.

You’ll find that `restoreSnapshotFn`, somewhat unexpectedly, accepts not
one but a list of snapshot objects. If you’re building a simple,
non-distributed source, this list will have just one element. However,
the same logic must work for distributed sources as well, and a
distributed source runs on many parallel processors at the same time.
Each of them will produce its own snapshot object. After a restart the
number of parallel processors may be different than before (because you
added a Jet cluster member, for example), so there’s no one-to-one
mapping between the processors before and after the restart. This is why
Jet passes all the snapshot objects to all the processors, and your
logic must work out which part of their data to use.

Here’s a brief example with a fault-tolerant streaming source that
generates a sequence of integers:

```java
StreamSource<Integer> faultTolerantSource = SourceBuilder
    .stream("fault-tolerant-source", processorContext -> new int[1])
    .<Integer>fillBufferFn((numToEmit, buffer) ->
        buffer.add(numToEmit[0]++))
    .createSnapshotFn(numToEmit -> numToEmit[0])
    .restoreSnapshotFn(
        (numToEmit, saved) -> numToEmit[0] = saved.get(0))
    .build();
```

The snapshotting function returns the current number to emit, the
restoring function sets the number from the snapshot to the current
state. This source is non-distributed, so we can safely do
`saved.get(0)`.
