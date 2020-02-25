---
title: Work with Apache Kafka
id: kafka
---

Kafka is often used in real-time streaming data architectures to provide
real-time analytics. Since Kafka is a fast, scalable, durable and
fault-tolerant publish-subscribe messaging system; Kafka is used in
use-cases where other messaging systems may not even be considered due
to volume and responsiveness.

Hazelcast Jet provides source and sink connectors for Kafka which are
suited for infinite stream processing jobs. The connectors also support
fault tolerance and snapshotting.

## Enrich Data using IMap

One of the most popular use-cases is enriching your data from an
external lookup table, in our case an `IMap`. Below code block shows
that we can easily read data from a topic, enrich the value by looking
up an IMap and write it to another topic.

```java
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("key.deserializer", StringDeserializer.class.getCanonicalName());
properties.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
properties.setProperty("key.serializer", StringSerializer.class.getCanonicalName());
properties.setProperty("value.serializer", StringSerializer.class.getCanonicalName());
Pipeline p = Pipeline.create();
p.readFrom(KafkaSources.<String, String>kafka(properties, "sourceTopic"))
 .withoutTimestamps()
 .mapUsingIMap("lookupMap", Map.Entry::getKey, (entry, lookupValue) -> entry(entry.getKey(), entry.getValue() + lookupValue))
 .writeTo(KafkaSinks.kafka(properties, "sinkTopic"));
JetInstance jet = Jet.bootstrappedInstance();
Job job = jet.newJob(p);
```

## Cache Data from Apache Kafka

Jet offers a convenient way to read and cache data from Kafka. With a
few lines of code you will be able to read a Kafka topic and materialize
it inside an in-memory key-value based storage.

```java
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("key.deserializer", StringDeserializer.class.getCanonicalName());
properties.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());

Pipeline p = Pipeline.create();
p.readFrom(KafkaSources.<String, String>kafka(properties, "sourceTopic"))
 .withoutTimestamps()
 .writeTo(Sinks.map("cacheMap"));

JetInstance jet = Jet.bootstrappedInstance();
Job job = jet.newJob(p);
```

## Complex Record Types

You can put your complex objects to Kafka using custom serializers and
de-serializers. Jet expects the consumed objects to be `Serializable`
since the objects will be moved between stages. You can also define a
projection function for source which will be applied to each record
before emitting to downstream. You can also define key and value
extractors for sink. Let's rewrite enrich data sample using a complex
object as the value of the record.

```java
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("key.deserializer", StringDeserializer.class.getCanonicalName());
properties.setProperty("value.deserializer", UserDeserializer.class.getCanonicalName());
properties.setProperty("key.serializer", StringSerializer.class.getCanonicalName());
properties.setProperty("value.serializer", UserSerializer.class.getCanonicalName());


Pipeline p = Pipeline.create();
p.readFrom(KafkaSources.kafka(properties, ConsumerRecord<String, User>::value, "sourceTopic"))
 .withoutTimestamps()
 .mapUsingIMap("passwordMap", User::getName, (user, pass) -> user.setPassword((byte[])pass))
 .writeTo(KafkaSinks.kafka(properties, "sinkTopic", User::getName, user -> user));

JetInstance jet = Jet.bootstrappedInstance();
Job job = jet.newJob(p);
```

The **User** class:

```java
public class User implements Serializable {

    private String name;
    private byte[] password;

    public User() {
    }

    public User(String name, byte[] password) {
        this.name = name;
        this.password = password;
    }

    public String getName() {
        return name;
    }

    public User setName(String name) {
        this.name = name;
        return this;
    }

    public byte[] getPassword() {
        return password;
    }

    public User setPassword(byte[] password) {
        this.password = password;
        return this;
    }
}
```

## Dependencies

To run the above sample code blocks you will need Hazelcast Jet Kafka
module.

<!--DOCUSAURUS_CODE_TABS-->
<!--Maven-->

```xml
<dependencies>
  <dependency>
      <groupId>com.hazelcast.jet</groupId>
      <artifactId>hazelcast-jet-kafka</artifactId>
      <version>4.0</version>
  </dependency>
</dependencies>
```

<!--Gradle-->

```bash
compile 'com.hazelcast.jet:hazelcast-jet-kafka:4.0'
```

<!--END_DOCUSAURUS_CODE_TABS-->