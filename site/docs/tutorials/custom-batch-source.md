---
title: Custom Batch Sources
description: Tutorial on how to define custom batch sources.
---

In the [Custom Sources and Sinks](../api/sources-sinks.md#custom-sources-and-sinks)
section of our [Sources and Sinks](../api/sources-sinks.md) programming
guide we have seen some basic examples of user-defined sources and
sinks. Let us now examine more examples which cover some of the
trickier aspects of writing our own sources. We will focus on batch
sources, ones reading bounded input data.

## 1. Start Hazelcast Jet

1. [Download](https://github.com/hazelcast/hazelcast-jet/releases/download/v4.0/hazelcast-jet-4.0.tar.gz)
  Hazelcast Jet

2. Unzip it:

```bash
cd <where_you_downloaded_it>
tar zxvf hazelcast-jet-4.0.tar.gz
cd hazelcast-jet-4.0
```

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
`custom-batch-source-tutorial` and copy the Gradle or Maven file
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

jar.manifest.attributes 'Main-Class': 'org.example.LineConsumer'
```

<!--Maven-->

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>custom-batch-source-tutorial</artifactId>
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
                            <mainClass>org.example.LineConsumer</mainClass>
                        </manifest>
                    </archive>
                </configuration>
            </plugin>
        </plugins>
    </build>
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

    static BatchSource<String> buildLineSource() {
        return SourceBuilder
                .batch("line-source", x -> new BufferedReader(
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

public class LineConsumer {

    public static void main(String[] args) {
        BatchSource<String> lineSource = Sources.buildLineSource();

        Pipeline p = Pipeline.create();
        p.readFrom(lineSource)
                .writeTo(com.hazelcast.jet.pipeline.Sinks.logger());

        JobConfig cfg = new JobConfig().setName("line-consumer");
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

This will produce a jar file called `custom-batch-source-tutorial-1.0-SNAPSHOT.jar`
in the `build/libs` folder of our project.

<!--Maven-->

```bash
mvn package
```

This will produce a jar file called `custom-batch-source-tutorial-1.0-SNAPSHOT.jar`
in the `target` folder or our project.

<!--END_DOCUSAURUS_CODE_TABS-->

## 6. Submit for Execution

Assuming our cluster is [still running](#1-start-hazelcast-jet) all we
need to issue is following command:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
<path_to_jet>/bin/jet submit build/libs/custom-batch-source-tutorial-1.0-SNAPSHOT.jar
```

<!--Maven-->

```bash
<path_to_jet>/bin/jet submit target/custom-batch-source-tutorial-1.0-SNAPSHOT.jar
```

<!--END_DOCUSAURUS_CODE_TABS-->

In the log of the Jet member we should see a message like this:

```text
...
Start executing job 'line-consumer', execution 03fd-a4ec-bd40-0001, execution graph in DOT format:
digraph DAG {
    "line-source" [localParallelism=1];
    "loggerSink" [localParallelism=1];
    "line-source" -> "loggerSink" [queueSize=1024];
}
...
```

> Note: we are assuming that we have a text file called `lines.txt` with
> lines in it present in the Jet members working directory.

The output you get should contain logged versions of the lines from
your `lines.txt` file. We had this (output produced by the custom sink
we have defined in [another tutorial](custom-sink.md)):

```text
... 1583315484503,SimpleEvent(timestamp=11:51:24.500, sequence=52134)
... 1583315484703,SimpleEvent(timestamp=11:51:24.700, sequence=52136)
... 1583315484903,SimpleEvent(timestamp=11:51:24.900, sequence=52138)
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

    static BatchSource<String> buildLineSource() {
        return SourceBuilder
                .batch("line-source", x -> new BufferedReader(
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
