---
title: SQL Introduction
description: The Hazelcast Jet SQL service documentation.
---

The service is in beta state. Behavior and API might change in future
releases. Binary compatibility is not guaranteed between minor or patch
releases.

Hazelcast can execute SQL statements using either the default SQL
backend contained in the Hazelcast IMDG code, or using the Jet SQL
backend in this package. The algorithm is this: we first try the
default backend, if it can't execute a particular statement, we try the
Jet backend.

For proper functionality the `hazelcast-jet-sql.jar` has to be on the
class path.

<!---
TODO confirm the jar setup. Also update JetSqlService.
-->

This documentation summarizes Hazelcast Jet SQL features. For a summary
of the default SQL engine features, see the documentation for the
`com.hazelcast.sql.SqlService` class.

## Overview

Hazelcast Jet is able to execute distributed SQL statements over any Jet
connector that supports the SQL integration. Currently those are:

- Local IMaps
- Apache Kafka topics
- Files (local and remote)

Each connector specifies its own serialization formats and a way of
mapping the stored objects to records with column names and SQL types.
See the individual connectors for details.

In the first release we support a very limited set of features,
essentially only reading and writing from/to the above connectors and
projection + filtering. Currently these are unsupported: joins,
grouping, aggregation. We plan to support these in the future.
