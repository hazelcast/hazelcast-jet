---
title: DDL statements
description: DDL commands for Hazelcast Jet SQL
---

## CREATE, DROP EXTERNAL MAPPING

### Introduction

The SQL language works with _tables_ that have a fixed list of columns
with data types. To use a remote object as a table, an _EXTERNAL
MAPPING_ must be created first (except for local IMaps, which can be
queried without creating a mapping first).

The mapping specifies the table name, an optional column list with types
and connection and other connector-specific parameters.

The mappings are created in the `public` schema. Jet doesn't support
user-created schemas currently. The implicit mappings for IMaps are
created in the `partitioned` schema. You can't drop mappings in this
schema, nor are they listed in the [information
schema](#information-schema) tables.

We currently do not support `ALTER MAPPING` command: use the `CREATE OR
REPLACE MAPPING` command to replace the mapping definition or DROP the
mapping first. A change to a mapping does not affect any jobs that are
already running based on it, only new jobs are affected.

### CREATE MAPPING Synopsis

```sql
CREATE [OR REPLACE] [EXTERNAL] MAPPING [IF NOT EXISTS] mapping_name
[ ( column_name column_type [EXTERNAL NAME external_name] [, ...] ) ]
TYPE type_identifier
[ OPTIONS ( option_name 'option_value' [, ...] ) ]
```

- `OR REPLACE`: overwrite the mapping if it already exists.

- `EXTERNAL`: an optional keyword, has no semantic meaning

- `IF NOT EXISTS`: do nothing if the external mapping already exists.

- `mapping_name`: an SQL identifier that identifies the mapping in SQL
  queries.

- `column_name`, `column_type`: the name and type of the column. For the
  list of supported types see the Hazelcast IMDG Reference Manual.

- `external_name`: the optional external name. If omitted,
  connector-specific rules are used to derive it from the column name.
  For example for key-value connectors such as IMap or Kafka, unless the
  column name is `__key` or `this`, it is assumed to be a field of the
  value. For other connectors it is assumed to be equal to
  `column_name`. See the connector specification for details.

- `type_identifier`: the connector type.

- `option_name`, `option_value`: a connector-specific option. For a list
  of possible options check out the connector javadoc. The `option_name`
  is an SQL identifier: it must be enclosed in double quotes if it
  contains special characters like `.`, `-` etc. The `option_value` is a
  regular SQL string literal enclosed in apostrophes.

#### Auto-resolving of columns and options

The columns in the column list are optional. Depending on the connector
it can resolve the columns based on the given options or by sampling a
random record in the target object. For example, if you give the java
class name for IMap value, we'll resolve the columns by reflecting that
class.

If the connector fails to resolve the columns, the statement will fail.
Check out individual connector documentation for details.

#### Example

In this example the field list and options are explicit.

```sql
CREATE MAPPING my_table(
    __key INT,
    valueField1 VARCHAR,
    valueField2 BIGINT EXTERNAL NAME "field-2"
)
TYPE IMap
OPTIONS (
    "serialization.key.format" 'java',
    "serialization.key.java.class" 'java.lang.Integer'
    "serialization.value.format" 'json'
)
```

For details regarding the above statement see the [IMap
connector](03-imap-connector.md) chapter.

### DROP MAPPING Synopsis

```sql
DROP [EXTERNAL] MAPPING [IF EXISTS] mapping_name
```

- `EXTERNAL`: an optional keyword, has no semantic meaning

- `IF EXISTS`: if the external mapping doesn't exist, do nothing; fail
  otherwise.

- `mapping_name`: the name of the mapping

## Information Schema

The information about existing mappings is available through
`information_schema` tables.

Currently, two tables are exposed:

- `mappings`: contains information about existing mappings

- `columns`: contains information about mapping columns

To query the information schema, use:

```sql
SELECT * FROM information_schema.mappings

SELECT * FROM information_schema.columns
```

## Custom connectors

Implementation of custom SQL connectors is currently not a public API,
we plan to define an API in the future.
