---
title: SQL Introduction
description: Introduction to Hazelcast Jet SQL features.
---

Hazelcast Jet SQL service executes distributed SQL statements over
Hazelcast collections and external data sets.

It allows you to specify Jet jobs using the well-known SQL language.

**Note:** _The service is in beta state. Behavior and API might change in
future releases. Binary compatibility is not guaranteed between minor or
patch releases._

## Overview

In the first release Jet SQL supports following features:

- SQL Queries over [Apache Kafka topics](05-kafka-connector.md) and
[Files (local and remote)](04-files-connector.md)
- Joining Kafka or file data with local IMaps (enrichment)
- Filtering and projection using [SQL
expressions](https://docs.hazelcast.org/docs/latest-dev/manual/html-single/index.html#expressions)
- Aggregating data from files using predefined
[aggregate functions](00a-basic-commands#aggregation-functions)
- Receiving query results via Jet client (Java) or writing the results
to an [IMap](03-imap-connector.md) in the Jet cluster
- Running continuous (streaming) and batch queries, see
[Job Management](02-job-management.md)

Notably, the following features are currently unsupported: joins
arbitrary external data sources, window aggregation, JDBC, sorting
(ORDER BY clause). We plan to support these in the future.

## Example: How to query Apache Kafka using SQL

Consider that we have a topic called `trades` that contains ticker
updates. Trades are encoded as JSON messages:

```json
{ "ticker": "ABCD", "price": 5.5 }
```

To use a remote topic as a table in Jet, an `EXTERNAL MAPPING` must be
created first. This maps the JSON messages to a fixed list of columns
with data types:

```sql
CREATE MAPPING trades (
    ticker VARCHAR,
    value DECIMAL)
TYPE Kafka
OPTIONS (
    valueFormat 'json',
    "bootstrap.servers" '1.2.3.4',
    "value.deserializer" 'org.apache.kafka.connect.json.JsonSerializer')
```

To submit the above query, use the Java API (we plan JDBC support and
support in non-Java clients in the future):

```java
JetInstance inst = ...;
inst.getSql().execute( /* query text */ );
```

A SQL query can now be used to read from the `trades` topic, as if it
was a table:

```java
JetInstance inst = ...;
try (SqlResult result = inst.getSql().execute("SELECT * FROM trades")) {
    for (SqlRow row : result) {
        // Process the row.
    }
}
```

The query now runs in the Jet cluster and streams the results to the Jet
client that started it. The iteration will never complete, a Kafka topic
is a _streaming source_, that is it has no end of data. The backing Jet
job will terminate when the client crashes, or add a `result.close()`
call to close it earlier. By default the reading starts at the tip of
the topic, but you can modify it by adding `auto.offset.reset` Kafka
property to toe mapping options.

Instead of sending results to the caller, Jet SQL query can also update
an IMap using the `SINK INTO` command. Jet SQL can thus be used as a
simple API to ingest data into Hazelcast. To be able to write to an
IMap, Jet has to know what objects to create for the map key and value.
It can derive that automatically by sampling an existing entry in the
map, but if the map is possibly empty, you have to create a mapping for
it too.

```sql
CREATE MAPPING latest_trades
TYPE IMap
OPTIONS (
    keyFormat 'java',
    keyJavaClass 'java.lang.String',
    valueFormat 'java',
    valueJavaClass 'java.math.BigDecimal'
)
```

Note that we omitted the column list in this query. It will be
determined automatically according to the options. Since we use `String`
and `BigDecimal` as the key and java class, the default name for key
(`__key`) and for the value (`this`) will be used. So the mapping will
behave as if the following columns were specified in the previous
statement:

```sql
(
    __key VARCHAR,
    this DECIMAL
)
```

The SINK INTO command will look like this:

```sql
SINK INTO latest_trades(__key, this)
SELECT ticker, value
FROM trades
```

It will put the `ticker` and `value` fields from the messages in the
topic into the `latest_trades` IMap. The `ticker` field will be used as
the map key and the `value` field will be used as the map value. If
multiple messages have the same ticker, the entry in the target map will
be overwritten, as they arrive.

However, you cannot directly execute the above command because it would
never complete. Remember, it's reading from a Kafka topic. Since it
doesn't return any rows to the client, you must create a job for it:

```sql
CREATE JOB trades_ingestion AS
SINK INTO latest_trades(__key, this)
SELECT ticker, value
FROM trades
```

The above command will create a job named `trades_ingestion`. Now, even
if the client disconnects, the cluster will continue running the job.

To cancel the job, use:

```sql
DROP JOB trades_ingestion
```

## Installation

For proper functionality the `hazelcast-jet-sql` has to be on the class
path. If you're using Gradle or Maven, make sure to add this module to
the dependencies.

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
compile 'com.hazelcast.jet:hazelcast-jet-sql:{jet-version}'
```

<!--Maven-->

```xml
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet-sql</artifactId>
    <version>{jet-version}</version>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

If you're using the distribution package, make sure to move the
`hazelcast-jet-sql-{jet-version}.jar` file from the `opt/` to `lib/`
directory.

## Hazelcast SQL and Jet SQL

Hazelcast can execute SQL statements using either the default SQL
backend contained in the Hazelcast IMDG, or using the Jet SQL backend.

Default SQL backend is designed for fast, online queries over Hazelcast
in-memory collections. Jet SQL backend allows you to combine Hazelcast
collections with external data sources (Kafka, files) using a single
query. It is designed for long-running queries (continuous queries,
batch processing). As such, Jet is not optimized for low overhead during
job submission so a tiny queries will not have great performance.

Hazelcast SQL service selects the backend automatically. The algorithm
is this: we first try the default IMDG backend, if it can't execute a
particular statement, we try the Jet backend.

This documentation summarizes the additional SQL features of Hazelcast
Jet. For a summary of the default SQL engine features, supported data
types and the built-in functions and operators please see the _SQL_
chapter in the [Hazelcast IMDG reference
manual](https://hazelcast.org/imdg/docs/).
