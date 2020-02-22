---
title: Sources and Sinks 
id: sources-sinks
---

Hazelcast Jet comes out of the box with many different sources and sinks
that you can work with, that are also referred to as _connectors_.

## Files

File sources generally involve reading a batch of files from either a
local/network disk or a distributed file system such as Amazon S3 or
Hadoop. Most file sources and sinks are batch oriented, but the sinks
that support _rolling_ capability can also be used as sinks in streaming
jobs.

### Local Disk

The simplest file source is designed to work with a local and network
file systems. This source is text-oriented and reads the file line by
line and emits a record per line:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.files("/home/data/web-logs"))
 .map(line -> LogParser.parse(line))
 .filter(log -> log.level().equals("ERROR"))
 .writeTo(Sinks.logger());
```

For CSV or JSON files it's possible to use the `filesBuilder` source:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.filesBuilder(sourceDir).glob("*.csv").build(path ->
    Files.lines(path).skip(1).map(SalesRecordLine::parse))
).writeTo(Sinks.logger());
```

For a local file system, the sources expect to see on each node just the
files that node should read. You can achieve the effect of a distributed
source if you manually prepare a different set of files on each node.
For shared file system, the sources can split the work so that each node
will read a part of the files.

#### File Sink

The file sink, like the source works with text and creates a line of
output for each record. When the rolling option is used it will roll the
filename to a new one once the criteria is met. It supports rolling by
size or date. The following will roll to a new file for every hour:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.itemStream(100))
 .withoutTimestamps()
 .writeTo(Sinks.filesBuilder("out")
 .rollByDate("YYYY-MM-dd.HH")
 .build());
```

Each node will write to a unique file with a numerical index. You can
achieve the effect of a distributed sink if you manually collect all the
output files on all members and combine their contents.

The sink also supports exactly-once processing and can work
transactionally.

#### File Watcher

File watcher is a streaming file source, where only the new files,
appended or changed lines are emitted. It expects that files are updated
in append-only fashion.

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.fileWatcher("/home/data"))
 .withoutTimestamps()
 .writeTo(Sinks.logger());
```

### Apache Avro

[Apache Avro](https://avro.apache.org/) is a binary data storage format
which is schema based. The connectors are similar to the local file
connectors, but works with binary files stored in _Avro Object Container
File_ format.

With Avro sources, you can use either the `SpecificReader` or
`DatumReader` depending on the data type:

```java
Pipeline p = Pipeline.create();
p.readFrom(AvroSources.files("/home/data", Person.class))
 .filter(person -> person.age() > 30)
 .writeTo(Sinks.logger());
```

The sink expects a schema and the type to be written:

```java
p.writeTo(AvroSinks.files(DIRECTORY_NAME, Person.getClassSchema()), Person.class))
```

### Hadoop InputFormat/OutputFormat

You can use Hadoop connector to read/write files from/to Hadoop
Distributed File System (HDFS), local file system, or any other system
which has Hadoop connectors including various cloud storages. Jet was
tested with:

* Amazon S3
* Google Cloud Storage
* Azure Cloud Storage
* Azure Data Lake

The Hadoop source and sink require a configuration object of type
[Configuration](https://hadoop.apache.org/docs/r2.10.0/api/org/apache/hadoop/conf/Configuration.html)
which supplies the input and output paths and formats. They don’t
actually create a MapReduce job, this config is simply used to describe
the required inputs and outputs. You can share the same `Configuration`
instance between several source/sink instances.

For example, to do a canonical word count on a Hadoop data source,
we can use the following pipeline:

```java
Job job = Job.getInstance();
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
TextInputFormat.addInputPath(job, new Path("input-path"));
TextOutputFormat.setOutputPath(job, new Path("output-path"));
Configuration configuration = job.getConfiguration();

Pipeline p = Pipeline.create();
p.readFrom(HadoopSources.inputFormat(configuration, (k, v) -> v.toString()))
 .flatMap(line -> traverseArray(line.toLowerCase().split("\\W+"))))
 .groupingKey(word -> word)
 .aggregate(AggregateOperations.counting())
 .writeTo(HadoopSinks.outputFormat(configuration));
```

The Hadoop source and sink will use either the new or the old MapReduce
API based on the input format configuration.

Each processor will write to a different file in the output folder
identified by the unique processor id. The files will be in a temporary
state until the job is completed and will be committed when the job is
complete. For streaming jobs, they will be committed when the job is
cancelled. We have plans to introduce a rolling sink for Hadoop in the
future to have better streaming support.

#### Data Locality

Jet will split the input data across the cluster, with each processor
instance reading a part of the input. If the Jet nodes are co-located
with the Hadoop data nodes, then Jet can make use of data locality by
reading the blocks locally where possible. This can bring a significant
increase in read throughput.

#### Serialization and Writables

Hadoop types implement their own serialization mechanism through the use
of `Writable` types. Jet provides an adapter to register a `Writable`
for [Hazelcast serialization](serialization) without having to write
additional serialization code. To use this adapter, you can register
your own `Writable` types by extending `WritableSerializerHook` and
registering the hook.

#### Hadoop Classpath

When submitting Jet jobs using Hadoop, sending Hadoop JARs should be
avoided and instead the Hadoop classpath should be used. Hadoop JARs
contain some JVM hooks and can keep lingering references inside the JVM
long after the job has ended, causing memory leaks.

To obtain the hadoop classpath, use the `hadoop classpath` command and
append the output to the `CLASSPATH` environment variable before
starting Jet.

### Amazon S3

The Amazon S3 connectors are text-based connectors that can read and
write files to Amazon S3 storage.

The connectors expect the user to provide either an S3Client client
instance or credentials (or using the default ones) to create the
client. The source and sink assume the data is in the form of plain text
and emit/receive data items which represent individual lines of text.

```java
AwsBasicCredentials credentials = AwsBasicCredentials.create("accessKeyId", "accessKeySecret");
S3Client s3 = S3Client.builder()
    .credentialsProvider(StaticCredentialsProvider.create(credentials))
    .build();

Pipeline p = Pipeline.create();
p.readFrom(S3Sources.s3(singletonList("input-bucket"), "prefix",
    () -> S3Client.builder().credentialsProvider(StaticCredentialsProvider.create(credentials)).build())
 .filter(line -> line.contains("ERROR"))
 .writeTo(Sinks.logger());
```

The S3 sink works similar to the local file sink, writing a line to the
output for each input item:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.items("the", "brown", "fox"))
 .writeTo(S3Sinks.s3("output-bucket", () -> S3Client.create()));
```

The sink creates an object in the bucket for each processor instance.
Name of the file will include a user provided prefix (if defined) and
followed by the processor’s global index, for example the processor
having the index `2` with prefix `my-object-` will create the object
`my-object-2`.

S3 sink uses multi-part upload feature of S3 SDK. The sink buffers the
items to parts and uploads them after buffer reaches to the threshold.
The multi-part upload is completed when the job completes and makes the
objects available on the S3. Since a streaming jobs never complete, S3
sink is not currently applicable to streaming jobs.

## Messaging Systems

### Apache Kafka

### JMS

### Apache Pulsar

>This connector is under incubation.

## In-memory data structures

### IMap

### IList

### ICache

### Reliable Topic

## Databases

### JDBC

### MongoDB

>This connector is under incubation.

### InfluxDB

>This connector is under incubation.

### Debezium

>This connector is under incubation.

### Redis

>This connector is under incubation.

## Miscellaneous

### Test Sources

### Socket

### Twitter

>This connector is under incubation.

## Custom Sources and Sinks

### SourceBuilder

### SinkBuilder