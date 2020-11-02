---
title: Basic commands
description: Description of SELECT, INSERT and SINK commands
---


## SELECT statement

### Synopsis

```sql
SELECT [ ALL | DISTINCT ] [ * | expression [ [ AS ] expression_alias ] [, ...] ]
FROM [schema_name.]table_name [ [ AS ] table_alias ]
[ WHERE condition ]
[ GROUP BY { expression | expression_index } [, ...] ]
[ HAVING condition [, ...] ]
```

The clauses above are standard SQL clauses. The `table_name` is a
mapping name, either as created using [DDL](01-ddl.md) or one created
automatically for non-empty IMaps.

Jet supports all operators and functions supported by IMDG. For
reference, find the _SQL_ section in the _Hazelcast IMDG Reference
Manual_. Additionally, Jet supports the aggregation functions.

### Aggregation functions

Jet supports these aggregation functions:

| Name<img width='350'/> | Description |
|--|--|
|`COUNT(*)` :: `BIGINT` | Computes the number of input rows. |
|`COUNT(any)` :: `BIGINT` | Computes the number of input rows in which the input value is not null. |
|`SUM(BIGINT)` :: `BIGINT`<br>`SUM(DECIMAL)` :: `DECIMAL`<br>`SUM(DOUBLE)` :: `DOUBLE` | Computes the sum of the non-null input values. |
|`AVG(DECIMAL)` :: `DECIMAL`<br>`AVG(DOUBLE)` :: `DOUBLE` | Computes the average (arithmetic mean) of all the non-null input values. |
|`MIN(any)` :: _same as input_ | Computes the minimum of the non-null input values. Applicable also to OBJECT type, if the underlying value is `java.lang.Comparable` |
|`MAX(any)` :: _same as input_ | Computes the maximum of the non-null input values. Applicable also to OBJECT type, if the underlying value is `java.lang.Comparable` |

Except for `COUNT` the functions return NULL when no rows are
aggregated. The functions cannot be applied to streaming inputs: they
need to accumulate the whole of input to produce some results and you
can't do this with input data. Currently, aggregation functions also
can't be used for data coming from IMaps because Jet currently doesn't
support reading from IMaps.

The argument to any aggregation function can be prepended with
`DISTINCT` keyword. In this case only distinct values are supplied to
the aggregation function. In case of MIN/MAX it makes no difference and
the keyword is ignored. For example, this query calculates the number of
distinct colors cars in the table have:

```sql
SELECT COUNT(DISTINCT color)
FROM cars
```

#### Memory considerations

The batch aggregation always maintains intermediate results in memory.
If you use the `DISTINCT` keyword, Jet also needs increased memory to
store the distinct values. Jet currently does not have any memory
management. If the number of groups in the result is high, it can lead
to `OutOfMemoryException`, after which the cluster might be unusable.
Also Jet never does streaming accumulation in case the input is already
sorted according to the grouping. This will be done in the future when
aggregation functions are supported by the default SQL engine.

### Isolation level

The isolation level is defined by the individual connectors used by
tables involved in the query. In general it's _read-committed_. One
aspect of this mode is that it doesn't prevent reading different
versions of a single row while executing a single query. In streaming
mode this behavior is even desired: for example, if you join a record
from an IMap to rows from a Kafka topic, this query can run for months
and you want to see the current version of the IMap entry, not the
version from the time when the query was started.

## INSERT/SINK statement

### Synopsis

```sql
[ INSERT | SINK ] INTO [schema_name.]table_name[(column_name [, ...])]
{ SELECT ... | VALUES(expression, [, ...]) }
```

Jet jobs typically read from some source(s) and write to a sink.
However, writing to the sink doesn't directly map to SQL commands. A Jet
sink isn't limited to only insert or delete rows, even the SQL standard
`MERGE` statement isn't easily applicable.

As a solution, Jet uses the non-standard `SINK INTO` command. Its
semantics is defined by the each connector. Jet takes the output of the
SELECT statement and sends it to the sink to process. For example, when
writing to IMap, the value associated with the key is overwritten, one
key can be overwritten multiple times.

Some connectors support the `INSERT INTO` statement. If they do, the
behavior is defined by the SQL standard. For example, the Apache Kafka
connector supports it. Jet doesn't support `DELETE` or `UPDATE`
statements.

### Transactional behavior

In SQL a statement is always atomic. In streaming SQL this is not
possible: a statement can run for infinite time, if it was atomic, we
will never see any result. Therefore Jet relaxed the behavior: the sink
is free to define its own transaction semantics. Due to support for
fault tolerance it might even have at-least-once semantics. If a query
fails, you might see partial results written.
