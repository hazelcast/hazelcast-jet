---
title: Work with Apache Kafka id: kafka
---

Apache Kafka is a messaging system which internally stores all the
messages as a distributed log. This makes it a great fit for a stream
processing system like Jet since it's able to store and replay several
hours or even days worths of messages.

Hazelcast Jet provides source and sink connectors for Kafka with
exactly-once processing guarantees that can be used as part of a
streaming job.

## Materialize Events from Apache Kafka

Using Jet's Kafka connector and a few lines of code, you can consume a
one or more Kafka topics, materialize and index it inside an in-memory
key-value based storage. For example, let's say you have a topic with
JSON values and want to cache it inside an `IMap`:

```java
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("key.deserializer", StringDeserializer.class.getCanonicalName());
properties.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
properties.setProperty("auto.offset.reset", "earliest");

Pipeline p = Pipeline.create();
p.readFrom(KafkaSources.<String, String>kafka(properties, "user-events"))
 .withoutTimestamps()
 .map(e -> {
     User user = User.parseFromJson(e.getValue());
     return entry(user.getId(), user);
 })
 .writeTo(Sinks.map("users"));

JetInstance jet = Jet.bootstrappedInstance();
Job job = jet.newJob(p);
```

Once the data is in the map, you can the map features for
[querying](../api/data-structures#querying) to access the data. The jet
job will keep running and update the map automatically as soon as new
messages arrive. Furthermore, it will automatically restart the job in
case of node failures and keep running without losing any messages.

It's also possible to pre-process the data by for example looking up
some static values using the
[`mapUsingIMap`](../api/stateful-stateless-transforms#mapUsingIMap)
operator.

## Fault Tolerance

You can enable fault tolerance for Kafka jobs by simply setting the
processing guarantee for a job. Jet will periodically take snapshots
storing the Kafka offsets along with the current computational state and
restart from the snapshot when a node joins or leaves the cluster.

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
