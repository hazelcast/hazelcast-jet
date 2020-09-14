---
title: DDL syntax
---

## CREATE, DROP EXTERNAL MAPPING

### Introduction

The SQL language works with _tables_ that have a fixed list of columns
with data types. To use a remote object as a table, an _external
mapping_ must be created first (except for local IMaps, which can be
queried without creating a mapping first).

The mapping specifies the table name, an optional column list with types
and connection and other parameters.

Be default, the mappings are created in the `public` schema. Jet doesn't
support user-created schemas currently. The implicit mappings for IMaps
are created in the `partitioned` schema. You can't drop mappings in this
schema, nor are they listed in the [information
schema](#information-schema).

We currently do not support `ALTER MAPPING` command, use the `CREATE OR
REPLACE MAPPING` command to replace the mapping definition or DROP the
mapping first. Changes to mappings do not affect any jobs that are
already running, only new jobs are affected.

### CREATE MAPPING Syntax

**CreateMapping ::=**
![CREATE MAPPING syntax](/docs/assets/ddl-CreateMapping.svg)

**ColumnList ::=**
![ColumnList syntax](/docs/assets/ddl-ColumnList.svg)

**TypeSpec ::=**
![TypeSpec syntax](/docs/assets/ddl-TypeSpec.svg)

**Options ::=**
![Options syntax](/docs/assets/ddl-Options.svg)

- `EXTERNAL`: an optional keyword, has no semantic meaning

- `OR REPLACE`: overwrite the mapping if it already exists.

- `IF NOT EXISTS`: do nothing if the external mapping already exists.

- `MappingName`: an SQL identifier that identifies the mapping in SQL
  queries.

- `ColumnName`, `ColumnType`: the name type of the column.

- `ExternalName`: the optional external name. If omitted, a
  connector-specific rules are used to derive it from the column name.
  For example for key-value connectors such as IMap or Kafka, unless the
  column name is `__key` or `this`, it is assumed to be a field of the
  value. See the connector specification for details.

- `TypeIdentifier`: the identifier of the connector type.

- `OptionName`, `OptionValue`: a connector-specific option. For a list
  of possible options check out the connector javadoc. The `OptionName`
  is a SQL identifier: must be enclosed in double quotes if it contains
  special characters like the period (`.`), dash (`-`) etc. The
  `OptionValue` is a regular SQL string.

## Auto-resolving of columns

The `ColumnList` is optional. If it is omitted, Jet will connect to the
remote system and try to resolve the columns in a connector-specific
way. For example, Jet can read a random message from a Kafka topic and
determine the column list from it. If Jet fails to resolve the columns
(most commonly if the remote object is empty), the DDL statement will
fail.

### DROP MAPPING Syntax

**DropMapping ::=**
![CREATE MAPPING syntax](/docs/assets/ddl-DropMapping.svg)

- `EXTERNAL`: an optional keyword, has no semantic meaning

- `IF EXISTS`: if the external mapping doesn't exist, do nothing; fail
  otherwise.

- `MappingName`: the name of the mapping

## CREATE/DROP JOB

Creates a job from a query that is not tied to the client session. When
you submit an INSERT query, its lifecycle is tied to the client session:
if the client disconnects, the query is cancelled, even though it
doesn't deliver results to the client.

If you want to submit a statement that is independent from the client
session, use the `CREATE JOB` command. Such a query will return quickly
and the job will run on the cluster. It can also be configured to be
fault tolerant.

**CreateJob ::=**
![CREATE JOB syntax](/docs/assets/ddl-CreateJob.svg)

**Options ::=**
![Options syntax](/docs/assets/ddl-Options.svg)

**DropJob ::=**
![DROP JOB syntax](/docs/assets/ddl-DropJob.svg)

- `JobName`: a unique name identifying the job.

- `QuerySpec`: the query to run by the job. It must not return rows to
  the client, that is it must not start with `SELECT`. Currently we
  support `INSERT INTO` or `SINK INTO` queries.

- `OptionName`, `OptionValue`: TODO

## Information Schema

The information about existing mappings is available through {@code
information_schema} tables.

Currently, 2 tables are exposed:

- `mappings` containing information about existing mappings

- `columns` containing information about mapping columns

## Custom connectors

Adding SQL mappings is currently not a public API, we plan to define an
API in the future.
