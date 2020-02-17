---
title: Kafka Connect Source
id: kafka-connect
---

## Background

From the [Confluent documentation](https://docs.confluent.io/current/connect/index.html):
> Kafka Connect, an open source component of Kafka, is a framework for
> connecting Kafka with external systems such as databases, key-value
> stores, search indexes, and file systems. Using Kafka Connect you can
> use existing connector implementations for common data sources and
> sinks to move data into and out of Kafka.

Take an existing Kafka Connect source and use it as a source for
Hazelcast Jet without the need to have a Kafka deployment. Hazelcast Jet
will drive the Kafka Connect connector and bring the data from external
systems to the pipeline directly.

## User Interaction

Sample Job ingests from RabbitMQ via Kafka Connect RabbitMQ Connector:

```java
Properties properties = new Properties();
properties.setProperty("name", "rabbitmq-source-connector");
properties.setProperty("connector.class", "com.github.jcustenborder.kafka.connect.rabbitmq.RabbitMQSourceConnector");
properties.setProperty("kafka.topic", "messages");
properties.setProperty("rabbitmq.queue", "test-queue");
properties.setProperty("rabbitmq.host", "???");
properties.setProperty("rabbitmq.port", "???");
properties.setProperty("rabbitmq.username", "???");
properties.setProperty("rabbitmq.password", "???");

Pipeline pipeline = Pipeline.create();
pipeline.readFrom(KafkaConnectSources.connect(properties))
        .withoutTimestamps()
        .map(record -> Values.convertToString(record.valueSchema(), record.value()))
        .writeTo(Sinks.logger());

JobConfig jobConfig = new JobConfig();
jobConfig.addJarsInZip("/path/to/kafka-connect-rabbitmq-0.0.2-SNAPSHOT.zip");

Job job = createJetMember().newJob(pipeline, jobConfig);
job.join();
```

Kafka Connect RabbitMQ Connector output in Jet Pipeline: 

```java
INFO: [127.0.0.1]:5701 [jet] [4.0-SNAPSHOT] Output to ordinal 0:
{
   "consumerTag":"amq.ctag-06l2oPQOnzjaGlAocCTzwg",
   "envelope":{
      "deliveryTag":100,
      "isRedeliver":false,
      "exchange":"ex",
      "routingKey":"test"
   },
   "basicProperties":{
      "contentType":"text/plain",
      "contentEncoding":"UTF-8",
      "headers":{
 
      },
      "deliveryMode":null,
      "priority":null,
      "correlationId":null,
      "replyTo":null,
      "expiration":null,
      "messageId":null,
      "timestamp":null,
      "type":null,
      "userId":"guest",
      "appId":null
   },
   "body":"Hello World!"

}
```

## Uploading Connectors to the Job Classpath

Since we are instantiating external Kafka Connect Connectors on the Jet
runtime, we need to be able to access those classes. Connectors are
usually shipped as a ZIP files. When using connectors with Kafka, they
need to be extracted to a certain path and that path needs to be
provided to the Kafka using `plugin.path` property. 

When used with Jet, we added a convenience to upload the ZIP file to Jet
classpath. One can use `addJarsInZip` method on JobConfig class. The JAR
files inside the ZIP file will be added to the classpath of the running
job. There is no standard structure inside the ZIP file as long as it
contains the JARs for connector implementation and it's dependencies, 

Sample Job Configuration with Connector ZIP Upload:

```java 
JobConfig jobConfig = new JobConfig();
jobConfig.addJarsInZip("/path/to/kafka-connect-rabbitmq-0.0.2-SNAPSHOT.zip");

Job job = createJetMember().newJob(pipeline, jobConfig);
job.join();
```

## Output Format

The source will emit items in the
[SourceRecord](https://docs.confluent.io/current/connect/javadocs/org/apache/kafka/connect/source/SourceRecord.html)
format, which details can be found below

| Field          | Description                                           |
|----------------|-------------------------------------------------------|
| topic          | the name of the topic; may be null                    |
| timestamp      | the timestamp; may be null                            |
| kafkaPartition | the partition number for the Kafka topic; may be null |
| key            | the key; may be null                                  |
| keySchema      | the schema for the key; may be null                   |
| value          | the value; may be null                                |
| valueSchema    | the schema for the value; may be null                 |
| headers        | the headers; may be null or empty                     |

The contents of the key, keySchema, value, and valueSchema fields are
changing between each connector because each different tool has its own
format for the emitted records.

## Technical Design

### Implementation

Hazelcast Jet drives the Kafka Connect connectors using the Kafka
Connect API with the SourceBuilder API. Connector classname is read from
the provided properties, an instance of it will be created and retrieved
one of it's defined tasks which are the actual work horses for Kafka
Connect API. Then we are repeatedly calling the `poll()` method on the
task. Each `SourceRecord` returned from the `poll()` has been emitted to
the downstream. The record's metadata has been collected to a local map
which later on plugged into the snapshotting mechanism. 

### Fault-Tolerance

The Kafka Connect connectors driven by Jet are participating to store
their state snapshots (e.g partition offsets + any metadata which they
might have to recover/restart) in Jet. This way when the job is
restarted they can recover their state and continue to consume from
where they left off. Since implementations may vary between Kafka
Connect modules, each will have different behaviors when there is a
failure. Please refer to the documentation of Kafka Connect connector of
your choice for detailed information.

### Testing Criteria

Integration tests added to codebase ( using testcontainers), which takes
a Kafka Connect connector and uses it as a source, then verifies the
items are emitted from the source.

## Future Work

Add support for distributed reading from Kafka, currently we are reading
the records from Kafka in a single task.
