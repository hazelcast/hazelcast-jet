---
title: Jet 4.5 Released
author: Marko Topolnik
authorURL: https://twitter.com/mtopolnik
authorImageURL: https://pbs.twimg.com/profile_images/922726943614783488/Pb5DDGWF_400x400.jpg
---

Today we're releasing Hazelcast Jet 4.5, the second release this year!
We're bringing Jet closer to IMDG, unifying their SQL syntax and
features. Our goal is to have a single SQL dialect that seamlessly uses
the features of both Jet and IMDG.

This version of Jet is built on Hazelcast IMDG 4.2, [check
out](https://hazelcast.com/blog/hazelcast-imdg-4-2-ga-is-released)
what's new there.

## Improved SQL Experience

There used to be an important inconvenience when using IMap from outside
Java &mdash; in order to populate it, you first had to create a Java
class for its value, or at best a portable `ClassDefinition`, which is
declarative but a part of server configuration. In either case you had
to restart the Hazelcast cluster before you could use the new record
type.

This is no longer needed: you can now use `CREATE MAPPING` and Jet SQL
will automatically translate it to the equivalent `ClassDefinition`.
This move finally closes the loop and makes the Hazelcast SQL experience
entirely independent of Java.

## Full Release Notes

Hazelcast Jet 4.5 is based on IMDG version 4.2. Check out its Release
Notes [here](https://docs.hazelcast.org/docs/rn/index.html#4-2) and,
for the Enterprise Edition,
[here](https://docs.hazelcast.org/docs/ern/index.html#4-2).

Members of the open source community that appear in these release notes:

- @hhromic

Thank you for your valuable contributions!

### New Features

- [sql] Dynamic `ClassDefinition` removes the need to restart the
  cluster in order to use a new record type in IMap (#2895)

### Enhancements

- [jdbc] @hhromic made the batch size limit configurable in the JDBC
  sink processor (#2888)
- [sql] Optimized the memory footprint of SQL aggregations (#2877)
- [sql] Now you can use expressions in SQL generator functions (#2944)
- [core] Pulling the data from an IMap/ICache could cause OOME due to
  creating too many partition iterators at the same time (#3009)
- [core] Removed misleading logging of errors that occur due to
  the job being cancelled (#2974)
- [pipeline-api] Significantly improved the performance of the `pickAny`
  aggregate operation in sliding windows (it lacked the _deduct_
  primitive) (#2917)

### Fixes

- [core] Jet's integration with the Java Logging Framework caused it to
  inadvertently close `System.out` from a  shutdown hook. This would
  then break other shutdown hooks. (#2649)
- [core] @hhromic fixed `DAG.toDotString()` to show the correct queue
  sizes (#2887)
- [sql] The `CREATE MAPPING` syntax had a fluke where you could use both
  `OR REPLACE` and `IF NOT EXISTS` in the same statement (#2921)
- [cdc] Implement processed offset feedback in CDC sources (#2854)
- [extensions] Updated AWS SDK version to 1.11.976. (#2989)
- [extensions] Updated Guava version to 30.1. (#2990)
- [extensions] Updated Parquet version to 1.12.0 (#3012)
- [extensions] Updated Avro to 1.10.2 (#2950)
- [extensions] Updated Jetty version to 9.4.38.v20210224 (#2993)
- [extensions] Updated wildfly-openssl to 1 (#2993)
- [extensions] Updated ElasticSearch-6 to 6.8.14 (#2993)
- [extensions] Updated ElasticSearch-7 to 7.10.0 (#2993)
- [extensions] Updated Kafka version to 2.2.2 (#2993)
- [extensions] Updated MySql Connector to 8.0.20 (#2993)
- [extensions] Updated Apache Http Client to 4.5.13 (#2993)
- [extensions] Updated Netty to 4.1.61.Final (#3023)
- [extensions] Updated Snakeyaml version to 1.26 [SEC-71] (#3024)

### Breaking Changes

The `DAG.toDotString(int defaultParallelism)` method signature is now
`DAG.toDotString(int defaultLocalParallelism, int defaultQueueSize)`.
Callers must now supply the queue size that will be shown if not
overriden on the edge.

_If you enjoyed reading this post, check out Jet at
[GitHub](https://github.com/hazelcast/hazelcast-jet) and give us a
star!_
