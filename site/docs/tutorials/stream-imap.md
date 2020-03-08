---
title: Stream Changes from IMap
---

[IMap](https://docs.hazelcast.org/docs/4.0/javadoc/com/hazelcast/map/IMap.html)
is the distributed data structure underlying Hazelcast. For Jet it's
also the most popular data source and sink.

The simplest scenario of using `IMap` as a source is when we treat the
entries it contains simply as a batch of data, read at a certain moment
of time. However Jet can make use of an `IMap` in a more advanced way,
treating all changes done to it (adding/updating/deleting entries) as
a stream of events which then can be processed further.

Let's see how this is done.

## 1. Start Hazelcast Jet

1. [Download](https://github.com/hazelcast/hazelcast-jet/releases/download/v4.0/hazelcast-jet-4.0.tar.gz)
  Hazelcast Jet

2. Unzip it:

```bash
cd <where_you_downloaded_it>
tar zxvf hazelcast-jet-4.0.tar.gz
cd hazelcast-jet-4.0
```

If you already have Jet and you skipped the above steps, make sure to
follow from here on.

3. Add following config block into the `config/hazelcast.yaml` file
   of your distribution (by default event journal is not enabled on
   `IMap`s, that's why we need to turn it on explicitly for the map we
   are going to use):

```yaml
  map:
    streamed-map:
      event-journal:
        enabled: true
```

4. Start Jet:

```bash
bin/jet-start
```

5. When you see output like this, Hazelcast Jet is up:

```text
Members {size:1, ver:1} [
    Member [192.168.1.5]:5701 - e7c26f7c-df9e-4994-a41d-203a1c63480e this
]
```

From now on we assume Hazelcast Jet is running on your machine.

## 2. Create a New Java Project

We'll assume you're using an IDE. Create a blank Java project named
`event-journal-tutorial` and copy the Gradle or Maven file
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
    compileOnly 'com.hazelcast.jet:hazelcast-jet:4.0'
}
```

<!--Maven-->

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>event-journal-tutorial</artifactId>
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
            <scope>provided</scope>
        </dependency>
    </dependencies>
</project>
```

<!--END_DOCUSAURUS_CODE_TABS-->

## 3. Write the Producer

In order to be able to have some live data in our `IMap` we will set up
a helper process, in the form of a Jet job, which will continuously
update entries in the map. Put following class into your project:

```java
package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;

import java.util.concurrent.ThreadLocalRandom;

public class Producer {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(100,
                (ts, seq) -> ThreadLocalRandom.current().nextLong(0, 1000)))
                .withoutTimestamps()
                .map(l -> Util.entry(l % 5, l))
                .writeTo(Sinks.map("streamed-map"));

        JobConfig cfg = new JobConfig().setName("producer");
        Jet.bootstrappedInstance().newJob(p, cfg);
    }

}
```

## 4. Write the Consumer

Our consumer will be a Jet job which will continuously consume update
events streamed by our map's event journal. Then it will print a
summary every second, specifying how many times each key has been
updated in the map during the past second.

```java
package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.WindowDefinition;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Consumer {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<Long, Long>mapJournal("streamed-map",
                JournalInitialPosition.START_FROM_CURRENT))
                .withIngestionTimestamps()
                .window(WindowDefinition.tumbling(TimeUnit.SECONDS.toMillis(1)))
                .groupingKey(Map.Entry::getKey)
                .aggregate(AggregateOperations.counting())
                .map(r -> String.format("Key %d had %d updates", r.getKey(), r.getValue()))
                .writeTo(Sinks.logger());

        JobConfig cfg = new JobConfig().setName("consumer");
        Jet.bootstrappedInstance().newJob(p, cfg);
    }

}
```

## 5. Submit for Execution

1. Build our Code:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
gradle build
```

This will produce a jar file called `event-journal-tutorial-1.0-SNAPSHOT.jar`
in the `build/libs` folder of our project.

<!--Maven-->

```bash
mvn package
```

This will produce a jar file called `event-journal-tutorial-1.0-SNAPSHOT.jar`
in the `target` folder or our project.

<!--END_DOCUSAURUS_CODE_TABS-->

2. Submit the Producer for Execution:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
<path_to_jet>/bin/jet submit \
    --class org.example.Producer \
    build/libs/event-journal-tutorial-1.0-SNAPSHOT.jar
```

<!--Maven-->

```bash
<path_to_jet>/bin/jet submit \
    --class org.example.Producer \
    target/event-journal-tutorial-1.0-SNAPSHOT.jar
```

<!--END_DOCUSAURUS_CODE_TABS-->

3. Submit the Consumer for Execution:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
<path_to_jet>/bin/jet submit \
    --class org.example.Consumer \
    build/libs/event-journal-tutorial-1.0-SNAPSHOT.jar
```

<!--Maven-->

```bash
<path_to_jet>/bin/jet submit \
    --class org.example.Consumer \
    target/event-journal-tutorial-1.0-SNAPSHOT.jar
```

<!--END_DOCUSAURUS_CODE_TABS-->

You should start seeing the one second summaries in the Jet member's
log:

```text
...
2020-03-06 10:17:21,025 ... Key 2 had 24 updates
2020-03-06 10:17:21,025 ... Key 1 had 19 updates
2020-03-06 10:17:21,025 ... Key 0 had 17 updates
2020-03-06 10:17:21,025 ... Key 3 had 14 updates
2020-03-06 10:17:21,025 ... Key 4 had 26 updates
...
```

## 6. Clean-up

To stop your Jet jobs from executing simply cancel them:

```bash
<path_to_jet>/bin/jet cancel consumer
<path_to_jet>/bin/jet cancel producer
```

Optionally you can also shut down the Jet member we have started:

```bash
<path_to_jet>/bin/jet-stop
```
