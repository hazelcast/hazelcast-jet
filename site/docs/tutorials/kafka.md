---
title: Process Data from Apache Kafka
---

Apache Kafka is a distributed, replayable messaging system. It is a
great fit for building a fault-tolerant data pipeline with Jet.

Here we'll build a Jet data pipeline that receives an event stream from
Kafka and computes its traffic intensity (events per second).

## 1. Create a New Java Project

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
plugins {
    id 'java'
}

group 'org.example'
version '1.0-SNAPSHOT'

sourceCompatibility = 1.8

repositories {
    mavenCentral()
}

dependencies {
    compile 'com.hazelcast.jet:hazelcast-jet:4.0'
    compile 'com.hazelcast.jet:hazelcast-jet-kafka:4.0'
}
```

<!--Maven-->

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>kafka-tutorial</artifactId>
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
        <dependency>
            <groupId>com.hazelcast.jet</groupId>
            <artifactId>hazelcast-jet-kafka</artifactId>
            <version>4.0</version>
        </dependency>
    </dependencies>
</project>
```

<!--END_DOCUSAURUS_CODE_TABS-->

## 2. Start Kafka

If you don't have it already, install and run Kafka using [these
instructions](https://www.tutorialkart.com/apache-kafka/install-apache-kafka-on-mac).

From now on we assume Kafka is running on your machine.

## 3. Publish an Event Stream to Kafka

This code publishes "tweets" (just some simple strings) to a Kafka topic
`tweets`, with varying intensity:

```java
package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import java.util.Properties;

public class TweetPublisher {
    public static void main(String[] args) throws Exception {
        String topicName = "tweets";
        Properties props = kafkaProps();
        try (KafkaProducer<Long, String> producer = new KafkaProducer<>(props)) {
            for (long eventCount = 0; ; eventCount++) {
                String tweet = String.format("tweet-%0,4d", eventCount);
                producer.send(new ProducerRecord<>(topicName, eventCount, tweet));
                System.out.format("Published '%s' to Kafka topic '%s'%n", tweet, topicName);
                Thread.sleep(20 * (eventCount % 20));
            }
        }
    }

    private static Properties kafkaProps() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", LongSerializer.class.getCanonicalName());
        props.setProperty("value.serializer", StringSerializer.class.getCanonicalName());
        return props;
    }
}
```

When you run it, you should see this in the output:

```text
Published 'tweet-0001' to Kafka topic 'tweets'
Published 'tweet-0002' to Kafka topic 'tweets'
Published 'tweet-0003' to Kafka topic 'tweets'
...
```

Let it run in the background while we go on to creating the next class.

## 4. Use Hazelcast Jet to Analyze the Event Stream

This code lets Jet connect to Kafka and show how many events per second
were published to the Kafka topic at a given time:

```java
package org.example;

import com.hazelcast.jet.*;
import com.hazelcast.jet.pipeline.Pipeline;
import org.apache.kafka.common.serialization.*;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static com.hazelcast.jet.kafka.KafkaSources.kafka;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.Sinks.logger;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;

public class JetJob {
    static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("HH:mm:ss:SSS");

    public static void main(String[] args) {
        String topicName = "tweets";

        Pipeline p = Pipeline.create();
        p.readFrom(kafka(kafkaProps(), topicName))
         .withNativeTimestamps(0)
         .window(sliding(1_000, 500))
         .aggregate(counting())
         .writeTo(logger(wr -> String.format(
                 "At %s Kafka got %,d tweets per second",
                 TIME_FORMATTER.format(LocalDateTime.ofInstant(
                         Instant.ofEpochMilli(wr.end()), ZoneId.systemDefault())),
                 wr.result())));

        JetInstance jet = Jet.bootstrappedInstance();
        Job job = jet.newJob(p);
        try {
            job.join();
        } finally {
            job.cancel();
            Jet.shutdownAll();
        }
    }

    private static Properties kafkaProps() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", LongDeserializer.class.getCanonicalName());
        props.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
        props.setProperty("auto.offset.reset", "earliest");
        return props;
    }
}
```

If you let `TweetPublisher` run while creating this class, you'll get
all the Kafka topic's history in the output. If you restart this program,
you'll get all the history again. This shows what it means for a source
to be replayable.

Sample output:

```text
16:11:35.033 ... At 16:11:27:500 Kafka got 3 tweets per second
16:11:35.034 ... At 16:11:28:000 Kafka got 2 tweets per second
16:11:35.034 ... At 16:11:28:500 Kafka got 8 tweets per second
```