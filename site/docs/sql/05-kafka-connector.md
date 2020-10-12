---
title: Apache Kafka Connector
description: Description of the SQL Apache Kafka connector
---

The Apache Kafka connector supports reading from and writing to local
Apache Kafka topics.

Apache Kafka is schema-less, however SQL assumes a schema. We assume all
messages in a topic are of the same type (with some exceptions). Kafka
also supports several serialization options, see below.

## Serialization options

The `keyFormat` and `valueFormat` options are mandatory. Currently, if
you create the mapping explicitly, we can't resolve these from a sample.

Possible values for `keyFormat` and `valueFormat`:

* `avro`
* `json`
* `java`

The key and value format can be different. All options not used by Jet
are passed directly to the Kafka producer or consumer. For an example see
 the examples at individual serialization options.

### Avro Serialization

When using Avro, Jet reads the fields from the `GenericRecord` returned
by the `KafkaAvroDeserializer`. When inserting to a topic, we create an
ad-hoc schema from the mapping columns with `jet.sql` as the name. If
you have a pre-existing schema that you wanted to use, Jet currently
can't create objects with your schema. However, it's possible to read
objects written using the ad-hoc schema with your schema if the fields
and types match.

#### Mapping Between SQL and Avro Types

| SQL Type | Avro Type |
| - | - |
| `TINYINT`<br/>`SMALLINT`<br/>`INT` | `INT` |
| `BIGINT` | `LONG` |
| `REAL` | `FLOAT` |
| `DOUBLE` | `DOUBLE` |
| `BOOLEAN` | `BOOLEAN` |
| `VARCHAR`<br/>and all other types | `STRING` |

All Avro types are a union of the `NULL` type and the actual type.

```sql
CREATE MAPPING my_topic (
)
TYPE Kafka
OPTIONS (
    keyFormat 'java',
    keyJavaClass 'java.lang.String',
    valueFormat 'avro',
    "bootstrap.servers" '10.0.1.120',
    "schema.registry.url" 'http://10.0.1.120:58819/'
    "key.serializer" 'org.apache.kafka.common.serialization.LongSerializer',
    "key.deserializer" 'org.apache.kafka.common.serialization.LongDeserializer',
    "value.serializer" 'io.confluent.kafka.serializers.KafkaAvroSerializer',
    "value.deserializer" 'io.confluent.kafka.serializers.KafkaAvroDeserializer',
    "auto.offset.reset" 'earliest'
    /* more Kafka options ... */
)
```

In this example, the key is a plain `Long` number, the value is
avro-serialized. `keyFormat`, `keyJavaClass` and `valueFormat` are
handled by Jet, the rest is passed to Kafka producer or consumer. The
option names containing the `.` must be enclosed in double quotes
because the `.` is a reserved character in SQL, double quotes are used
to quote identifiers: this way the `.` is interpreted literally.

The Apac

### JSON serialization

For the JSON format no additional options are required. However, the
column list can't be automatically determined, it must be explicitly
specified when creating the mapping.

```sql
CREATE MAPPING my_map(
    __key BIGINT,
    field1 VARCHAR,
    field2 INT)
TYPE IMap
OPTIONS (
    keyFormat 'json',
    valueFormat 'json')
```

The JSON type system doesn't exactly match SQL's. For example, while
JSON has unlimited precision for numbers, such numbers are not typically
portable. The integer and floating-point types produce a JSON number.
The `DECIMAL` type is stored as a JSON string. All temporal types are
also stored as a JSON string.

We don't provide first-class JSON type that's specified by the SQL
standard. That means you can't use functions like `JSON_VALUE` or
`JSON_QUERY`. If your JSON documents don't all have the same fields, the
usage is limited.

Internally, JSON values are stored in the string form.

### Java Serialization

This is the last-resort serialization option. It uses the Java object
exactly as it is returned by the `ConsumerRecord.key()` and `value()`
methods.

For this format you must specify the class name using `keyJavaClass` and
`valueJavaClass` options, for example:

```sql
CREATE MAPPING my_map
TYPE IMap
OPTIONS (
    keyFormat 'java',
    keyJavaClass 'java.lang.Long',
    valueFormat 'java',
    valueJavaClass 'com.example.Person')
```

If the Java class is the class belonging to some primitive type, that
type will directly be used for the key or value and mapped under either
`__key` or `this` column name (`this` is used for the value itself). In
the example above, the key will be mapped under the `BIGINT` type.

If the Java class is not one of the primitive types, Hazelcast will
analyze the class using reflection and its properties will be used as
column names. Public fields and get-methods will be used. If some
property has non-primitive type, it will be mapped under the `OBJECT`
type.

## External Name

You rarely need to specify the columns in DDL. If you do, you might need
to specify the external name.

The messages in Kafka have _key_ and _value_ elements. Because of this,
the external name must always start with either `__key` or `this`.

The external name is optional for a column, it defaults to
`this.<columnName>` (except for the `__key` and `this` columns). So
normally you only need to specify it for key fields.

## Heterogeneous Messages

If you have messages of different types in your topic, you have to
specify the columns in the `CREATE MAPPING` command. You may specify
columns that don't exist in some messages. If a property doesn't exist
in a specific message instance, NULL is returned for that column.

For example, let's say you have these messages in your topic:

|key|value|
|-|-|
|1|`{"name":"Alice","age":42}`|
|2|`{"name":"Bob","age":43,"petName":"Zaz"}`|

If you map the column `petName`, it will contain `null` for the entry
with `key=1`, even though it doesn't exist in that value.

This works similarly for differently-serialized objects: we'll try the

### TODO test this

extract the field by name, regardless of the actual serialization format
encountered at runtime. The specified serialization format will be used
when writing into that topic.
