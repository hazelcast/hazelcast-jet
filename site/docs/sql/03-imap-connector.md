---
title: IMap Connector
description: Description of the SQL IMap connector
---

The IMap connector supports reading from and writing to local IMaps.
Remote IMaps are not supported yet.

Local IMaps are automatically mapped in the `partitioned` schema. To
query a local IMap, you can directly execute a statement against it. If
a schema isn't specified, the object is first looked up in the `public`
schema (if a mapping was explicitly created) and then in the
`partitioned` schema.

IMap is schema-less, however SQL assumes a schema. We assume all entries
in the map are of the same type (with some exceptions). IMap also
supports several serialization options, see below.

If you don't create the mapping for an IMap explicitly, we'll sample an
arbitrary record to derive the schema and serialization type. You need
to have at least one record on each cluster member for this to work. If
you can't ensure this, create the mapping explicitly.

## Serialization options

The `keyFormat` and `valueFormat` options are mandatory. Currently, if
you create the mapping explicitly, we can't resolve these from a sample.

Possible values for `keyFormat` and `valueFormat`:

* `portable`
* `json`
* `java`

The key and value format can be different.

### `Portable` serialization

For this format, you need to specify additional options:

* `key-` and `valuePortableFactoryId`
* `key-` and `valuePortableClassId`
* `key-` and `valuePortableVersion`: optional, defaults to `0`

The column list will be resolved by looking at the `ClassDefinition`
found using the given factoryId, classId and version.

The benefit of this format is that it doesn't deserialize the whole key
or value when reading only a subset of fields. Also it doesn't require a
custom Java class to be defined on the cluster, so it's usable for
non-Java clients.

Example mapping where both key and value are `Portable`:

```sql
CREATE MAPPING my_map
TYPE IMap
OPTIONS (
    keyFormat 'portable',
    keyPortableFactoryId '123',
    keyPortableClassId '456',
    keyPortableVersion '0',  -- optional
    valueFormat 'portable',
    valuePortableFactoryId '123',
    valuePortableClassId '789',
    valuePortableVersion '0',  -- optional
)
```

For more information on `Portable` see Hazelcast IMDG Reference Manual.

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
exactly as it is returned by the `IMap.get()` call. It's used for
objects serialized using the Java serialization or using Hazelcast
custom serialization (`DataSerializable` or
`IdentifiedDataSerializable`).

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

The entries in a map naturally have _key_ and _value_ elements. Because
of this, the external name must always start with either `__key` or
`this` (which denotes the map value).

The external name is optional for a column, it defaults to
`this.<columnName>` (except for the `__key` and `this` columns). So
normally you only need to specify it for key fields.

## Heterogeneous Maps

If you have values of different type in your map, you have to specify
the columns in the `CREATE MAPPING` command. Otherwise different columns
will be resolved based on which sample entry is chosen.

If you specify columns manually, you may specify columns that don't
exist in some values. If a property doesn't exist in a specific value
instance, NULL is returned for that column.

For example, let's say you have this IMap:

|key|value|
|-|-|
|1|`{"name":"Alice","age":42}`|
|2|`{"name":"Bob","age":43,"petName":"Zaz"}`|

If you map the column `petName`, it will contain `null` for the entry
with `key=1`, even though it doesn't exist in that value.

This works similarly for differently-serialized objects: we'll try the
extract the field by name, regardless of the actually serialization
format encountered at runtime. The specified serialization format will
be used when inserting into that map.

## Notes

Remote IMaps are not yet supported.
