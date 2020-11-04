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

## Example: How to query Kafka using SQL

Consider that we have a topic called `trades` that contain ticker
updates. Trades are encoded as JSON messages:

```json
{ "ticker": "ABCD", "value": 5.5 }
```

To use a remote topic as a table in Jet, an EXTERNAL MAPPING must be
created first. This maps the JSON messages to a fixed list of columns
with data types:

```sql
CREATE MAPPING trades (
    ticker VARCHAR,
    value DECIMAL)
TYPE Kafka
OPTIONS (
-- TODO can we omit the key??
    valueFormat 'json',
    "bootstrap.servers" '1.2.3.4',
    "value.deserializer" 'org.apache.kafka.connect.json.JsonSerializer')
```

Jet SQL queries can now use the `trades` as a table:

```java
JetInstance inst = ...;
try (SqlResult result = inst.getSql().execute("SELECT * FROM trades")) {
    for (SqlRow row : result) {
        // Process the row.
    }
}
```

The query now runs in the Jet cluster and streams results to the Jet
client that started it. The iteration will never complete

Instead of sending results to the caller, Jet SQL query can also update
the IMap using the `SINK INTO` command. Jet SQL can thus be used as a
simple API to ingest data into Hazelcast. To be able to write to an
IMap, Jet has to know what objects to create for the map key and value.
It can derive that automatically by sampling an existing entry in the
map, but if the map is possibly empty, you have to create a mapping for
it too.

```sql
inst.getSql().execute("CREATE MAPPING latest_trades "
    + "TYPE IMap "
    + "OPTIONS ("
    + "keyFormat 'java',"
    + "keyJavaClass 'java.lang.String',"
    + "valueFormat 'java',"
    + "valueJavaClass 'java.math.BigDecimal'"
    + ")"
);
```

Note that we omitted the column list in this query. It will be
determined automatically according to the options. Since we use `String`
and `BigDecimal` as the key and java class, the default name for key
(`__key`) and for the value (`this`) will be used. So the mapping will
behave as if these these two columns were specified in the previous
statement:

```sql
(
    __key VARCHAR,
    this DECIMAL
)
```

Now we can try to submit the `SINK INTO`. But since the `trades` is an
Apache Kafka topic, :

```java
inst.getSql().execute("SINK INTO latest_trades SELECT * FROM trades");
```

But what happened? It threw this exception:

```plain
You must use CREATE JOB statement for a streaming DML query
```

Jet will run the query in the cluster until it's terminated. Use Jet
client (Java) or the Management Center tool to control and manage the
queries.

TODO: link code samples.

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

Hazelcast can execute SQL statements using either the
[default SQL backend](https://docs.hazelcast.org/docs/latest-dev/manual/html-single/index.html#sql)
contained in the Hazelcast IMDG, or using the Jet SQL backend.

Default SQL backend is designed for fast, online queries over Hazelcast
in-memory collections. Jet SQL backend allows you to combine Hazelcast
collections with external data sources (Kafka, files) using a single
query. It is designed to be used for long-running queries (continuous
queries, batch processing). As such, Jet is not optimized for low
overhead during job submission.

Hazelcast SQL service selects the backend automatically.
The algorithm is this: we first try the default IMDG backend, if it
can't execute a particular statement, we try the Jet backend.

This documentation summarizes the additional SQL features of Hazelcast
Jet. For a summary of the default SQL engine features, supported data
types and the built-in functions and operators please see the
[SQL engine the reference
manual](https://docs.hazelcast.org/docs/latest-dev/manual/html-single/index.html#sql).
