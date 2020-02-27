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

## Schema Registry

While you can use custom serializer/de-serializer for your complex
objects, you can store your object as `json` or `avro` and use a **Schme
Registry** to handle the metadata.

```java
Pipeline p = Pipeline.create();

Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("key.deserializer", StringDeserializer.class.getCanonicalName());
properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getCanonicalName());
properties.setProperty("schema.registry.url", "http://localhost:8081");
properties.setProperty("specific.avro.reader", "true");

p.readFrom(KafkaSources.<String, User>kafka(properties, "sourceTopic"))
 .withoutTimestamps()
 .writeTo(Sinks.logger());
```

The **User** class with avro schema:

```java
public class User implements Serializable, SpecificRecord {

    private static final Schema SCHEMA = SchemaBuilder
            .record(User.class.getSimpleName())
            .namespace(User.class.getPackage().getName())
            .fields()
                .name("username").type().stringType().noDefault()
                .name("password").type().stringType().noDefault()
            .endRecord();

    private String username;
    private String password;

    public User() {
    }

    public User(String username, String password, int age, boolean status) {
        this.username = username;
        this.password = password;
    }

    @Override
    public void put(int i, Object v) {
        switch (i) {
            case 0:
                username = v.toString();
                break;
            case 1:
                password = v.toString();
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public Object get(int i) {
        switch (i) {
            case 0:
                return username;
            case 1:
                return password;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public Schema getSchema() {
        return SCHEMA;
    }

    @Override
    public String toString() {
        return "avro.model.User{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}

```

## Fault Tolerance

Jet provides fault tolerance by saving the state of the stages
periodically to a *snapshot*. In case of a failure the job is started
from the last stored offsets automatically. You have to configure a
processing guarantee and a snapshot interval when submitting the job.

```java
JobConfig jobConfig = new JobConfig()
        .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
        .setSnapshotIntervalMillis(5000);

Job job = jet.newJob(p, jobConfig);
```

## Dependencies

To run the above sample code blocks you will need Hazelcast Jet Kafka
module.

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
compile 'com.hazelcast.jet:hazelcast-jet-kafka:4.0'
```

<!--Maven-->

```xml
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet-kafka</artifactId>
    <version>4.0</version>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

If you are using the downloaded binaries you can find the Kafka module
(`hazelcast-jet-kafka-4.0.jar`) in `opt` directory. Moving it to `lib`
directory will be enough to put it to the classpath.
