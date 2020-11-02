---
title: SQL Introduction
description: Introduction to Hazelcast Jet SQL features.
---


The SQL service provided by Hazelcast Jet allows you to specify Jet jobs
declaratively.

**Note:** _The service is in beta state. Behavior and API might change in
future releases. Binary compatibility is not guaranteed between minor or
patch releases._

Hazelcast can execute SQL statements using either the default SQL
backend contained in the Hazelcast IMDG, or using the Jet SQL backend.
The algorithm is this: we first try the default backend, if it can't
execute a particular statement, we try the Jet backend.

### Installation

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

### IMDG and Jet SQL Feature Disparity

This documentation summarizes the additional SQL features of Hazelcast
Jet. For a summary of the basic SQL engine features see the javadoc for
the `com.hazelcast.sql.SqlService` class and the _SQL_ chapter in the
[Hazelcast IMDG Reference Manual](https://hazelcast.org/imdg/docs/).
There you can find the following:

- Supported data types
- List of built-in functions and operators

Currently, there are SQL features that are supported by IMDG, but not
supported by Jet. Most notably, Jet doesn't yet support reading from
IMaps. In other words, Jet SQL features are not a superset of IMDG SQL
features. We're working on more features and closing this gap.

Another difference is that every `SELECT` or DML query is backed by a
Jet job. Jet is not optimized for low overhead during job submission and
completion, but for longer-lasting jobs. Even though you can insert
records one by one using the `INSERT ... VALUES` statement, the
performance will be very low. On the other hand, the performance of
`INSERT ... SELECT` statements is good because the whole statement is
executed as a single Jet job and the startup overhead is negligible.

## Overview

Hazelcast Jet is able to execute distributed SQL statements using any
Jet connector that supports the SQL integration. Currently those are:

- [Local IMaps](03-imap-connector.md) - writing only
- [Apache Kafka topics](05-kafka-connector.md)
- [Files (local and remote)](04-files-connector.md) - reading only

Each connector specifies its own serialization formats and a way of
mapping the stored objects to records with column names and SQL types.
See the documentation for individual connectors in subsequent chapters
for details.

In the first release we support a very limited set of features,
essentially only reading and writing from/to the above connectors and
projection, filtering and batch aggregation. Notably, the following
features are currently unsupported: joins, window aggregation. We plan
to support these in the future.

## API to Execute SQL

To submit a query, use:

```java
JetInstance inst = ...;
try (SqlResult result = inst.getSql().execute("SELECT ...")) {
    for (SqlRow row : result) {
        // Process the row.
    }
}

```

If your statement doesn't return rows, you can avoid the
try-with-resources clause:

```java
inst.getSql().execute("CREATE MAPPING ...");
```

Hazelcast doesn't currently support JDBC (it's planned for the future).
Identifiers such as table and column names are case-sensitive. Function
names and SQL keywords aren't. If your identifier contains special characters,
use `"` to quote. For example, if your map is named `my-map`:

```sql
SELECT * FROM "my-map";  -- works
sElEcT * from "my-map";  -- works
SELECT * FROM my-map;    -- fails, `-` interpreted as subtraction
SELECT * FROM "MY-MAP";  -- fails, map name is case-sensitive
```
