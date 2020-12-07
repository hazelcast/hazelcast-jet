---
title: File Connector
description: Description of the SQL File connector
---

The File connector supports reading from both local and remote files.

To work with files you must specify location and serialization.
Any options not recognized by Jet are passed, in case of remote files,
directly to Hadoop client. For local files they are simply ignored.

You can find detailed information for the options below.

## Location options

`path` is an absolute path to the directory containing your data. These
are the supported schemes:

* `hdfs`: HDFS
* `s3a`: Amazon S3
* `wasbs`: Azure Cloud Storage
* `adl`: Azure Data Lake Generation 1
* `abfs`: Azure Data Lake Generation 2
* `gs`: Google Cloud Storage

So for instance, path to data residing in a Hadoop cluster could look
like `hdfs://path/to/directory/`. Any path not starting with any of the
above is considered local (i.e. files residing on Jet members), e.g.
`/path/to/directory/`.

`glob` is a pattern to filter the files in the specified directory.
The default value is '*', matching all files.

## Serialization options

`format` defines the serialization used to read the files. We assume all
records in files to be of the same format. These are the supported
`format` values:

* `csv`
* `jsonl`
* `avro`
* `parquet`: remote files only

For each serialization format you can skip the mapping columns. When
you do so, we sample one of the files and infer columns from there.
In case of local files, it's important to have some data on each
member, otherwise the query might fail.

See the examples for individual serialization options below.

### CSV Serialization

The `csv` files are expected to be comma separated and `UTF-8` encoded.
If you skip mapping columns from the declaration, your files must
have a header as we infer column names from there. All inferred columns
are of `VARCHAR` type.

```sql
CREATE MAPPING my_files
TYPE File
OPTIONS (
    'path' = '/path/to/directory',
    'format' = 'csv'
)
```

### JSONL serialization

The `jsonl` files are expected to contain one valid json document per
line and be `UTF-8` encoded. If you skip mapping columns from the
declaration, we infer names and types based on a sample.

```sql
CREATE MAPPING my_files
TYPE File
OPTIONS (
    'path' = '/path/to/directory',
    'format' = 'jsonl'
)
```

#### Mapping Between JSON and SQL Types

| JSON type | SQL Type  |
| - | - |
| `BOOLEAN` | `BOOLEAN` |
| `NUMBER` | `DOUBLE` |
| `STRING` | `VARCHAR` |
| all other types | `OBJECT` |

### Avro & Parquet Serialization

The `avro` & `parquet` files are expected to contain Avro records. If
you skip mapping columns from the declaration we infer names and types
based on a sample.

```sql
CREATE MAPPING my_files
TYPE File
OPTIONS (
    'path' = '/path/to/directory',
    'format' = 'avro'
)
```

```sql
CREATE MAPPING my_files
TYPE File
OPTIONS (
    'path' = 'hdfs://path/to/directory',
    'format' = 'parquet'
    /* more Hadoop options ... */
)
```

#### Mapping Between Avro and SQL Types

| Avro Type | SQL Type |
| - | - |
| `BOOLEAN` | `BOOLEAN` |
| `INT` | `INT` |
| `LONG` | `BIGINT` |
| `FLOAT` | `REAL` |
| `DOUBLE` | `DOUBLE` |
| `STRING` | `VARCHAR` |
| all other types | `OBJECT` |

## External Name

You rarely need to specify the columns in DDL. If you do, you might want
to specify the external name. External names are supported for all
formats except for `csv`.

We don't support nested fields, hence the external name should refer to
the top-level field - not containing any `.`.

## File Table Functions

To execute an ad hoc query against data in files you can use one of the
predefined table functions:

* `csv_file`
* `jsonl_file`
* `avro_file`
* `parquet_file`

Table functions will create a temporary mapping, valid for the duration
of the statement. They accept the same options as `CREATE MAPPING`
statements.

You can use positional arguments:

```sql
SELECT * FROM TABLE(
  CSV_FILE('/path/to/directory', '*.csv', MAP['key', 'value'])
)
```

Or named arguments:

```sql
SELECT * FROM TABLE(
  CSV_FILE(path => '/path/to/directory', options => MAP['key', 'value'])
)
```

## Installation

Depending on what formats you want to work with you need different
modules on the classpath.

`csv` format requires the `hazelcast-jet-csv` module:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
compile 'com.hazelcast.jet:hazelcast-jet-csv:{jet-version}'
```

<!--Maven-->

```xml
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet-csv</artifactId>
    <version>{jet-version}</version>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

`avro` format requires the `hazelcast-jet-avro` module:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
compile 'com.hazelcast.jet:hazelcast-jet-avro:{jet-version}'
```

<!--Maven-->

```xml
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet-avro</artifactId>
    <version>{jet-version}</version>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

`parquet` format and all remote files require the `hazelcast-jet-hadoop`
module:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
compile 'com.hazelcast.jet:hazelcast-jet-hadoop:{jet-version}'
```

<!--Maven-->

```xml
<dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet-hadoop</artifactId>
    <version>{jet-version}</version>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

If you're using the distribution package, make sure to move the
respective jar files from the `opt/` to the `lib/` directory.
