---
title: Serialization
id: serialization
---

## (De)Serialization in Jet

To be able to send object state over a network or store it in a file
one has to first serialize it into raw bytes. Similarly, to be able to
fetch an object state over a wire or read it from a persistent storage
one has to deserialize it from raw bytes first. As Hazelcast Jet is a
distributed system by nature (de)serialization is integral part of it.
Understanding, when it is involved, how does it support the pipelines
and knowing differences between each of the strategies is crucial to
efficient usage of Hazelcast Jet.

Hazelcast Jet closely integrates with Hazelcast IMDG exposing many of
its features to Jet users. In particular, one can use IMDG data
structure as Jet `Source` and/or `Sink`. Objects retrieved from and
stored in those have to be (de)serializable.

Another case which might require (de)serializable objects is sending
computation results between remote vertices. Hazelcast Jet tries to
minimize network traffic as much as possible, nonetheless different
parts of a [DAG](concepts/dag.md) can reside on separate cluster members.
To catch (de)serialization issues early on, we recommend using a
2-member local Jet cluster for development and testing.

Currently, Hazelcast Jet supports 6 interfaces to (de)serialize objects:

- [java.io.Serializable](https://docs.oracle.com/javase/8/docs/api/java/io/Serializable.html)
- [java.io.Externalizable](https://docs.oracle.com/javase/8/docs/api/java/io/Externalizable.html)
- [com.hazelcast.nio.serialization.Portable](https://docs.hazelcast.org/docs/latest/javadoc/com/hazelcast/nio/serialization/Portable.html)
- [com.hazelcast.nio.serialization.DataSerializable](https://docs.hazelcast.org/docs/latest/javadoc/com/hazelcast/nio/serialization/DataSerializable.html)
- [com.hazelcast.nio.serialization.IdentifiedDataSerializable](https://docs.hazelcast.org/docs/latest/javadoc/com/hazelcast/nio/serialization/IdentifiedDataSerializable.html)
- [com.hazelcast.nio.serialization.StreamSerializer](https://docs.hazelcast.org/docs/latest/javadoc/com/hazelcast/nio/serialization/StreamSerializer.html)
 & [com.hazelcast.nio.serialization.ByteArraySerializer](https://docs.hazelcast.org/docs/latest/javadoc/com/hazelcast/nio/serialization/ByteArraySerializer.html)

The following table provides a comparison between them to help you in
deciding which interface to use in your applications.
|      Serialization interface      |                                                                      Advantages                                                                      |                                               Drawbacks                                              |
|:---------------------------------:|:----------------------------------------------------------------------------------------------------------------------------------------------------:|:----------------------------------------------------------------------------------------------------:|
|            Serializable           | <ul><li>Easy to start with, requires no implementation</li></ul>                                                                                     | <ul><li>CPU intensive</li><li>Space inefficient</li></ul>                                            |
|           Externalizable          | <ul><li>Faster and more space efficient than Serializable</li></ul>                                                                                  | <ul><li>CPU intensive</li><li>Space inefficient</li><li>Requires implementation</li></ul>            |
|              Portable             | <ul><li>Faster and more space efficient than java standard interfaces</li><li>Supports versioning</li><li>Supports partial deserialization</li></ul> | <ul><li>Requires implementation</li><li>Requires factory registration during cluster setup</li></ul> |
|          DataSerializable         | <ul><li>Faster and more space efficient than java standard interfaces</li></ul>                                                                      | <ul><li>Requires implementation</li></ul>                                                            |
|     IdentifiedDataSerializable    | <ul><li>Relatively fast and space efficient</li></ul>                                                                                                | <ul><li>Requires implementation</li><li>Requires factory registration during cluster setup</li></ul> |
| [Stream&#124;ByteArray]Serializer | <ul><li>The fastest and lightest out of supported interfaces</li></ul>                                                                               | <ul><li>Requires implementation</li><li>Requires registration during cluster setup</li></ul>         |

Below you can find rough performance numbers one can expect when
employing each of those strategies. A straightforward
[benchmark](https://github.com/hazelcast/hazelcast/blob/master/hazelcast/src/test/java/com/hazelcast/serialization/SerializationBenchmark.java)
which continuously serializes and then deserializes very simple object:

```java
class Person {
    private String firstName;
    private String lastName;
    private int age;
    private float height;
}
```

counting the total throughput, yields following results:

```text
# Processor: Intel(R) Core(TM) i7-4700HQ CPU @ 2.40GHz
# VM version: JDK 13, OpenJDK 64-Bit Server VM, 13+33

Benchmark                                           Mode  Cnt        Score   Error  Units
SerializationBenchmark.serializable                thrpt    2   236398.984          ops/s
SerializationBenchmark.externalizable              thrpt    2   651959.024          ops/s
SerializationBenchmark.portable                    thrpt    2   821097.835          ops/s
SerializationBenchmark.dataSerializable            thrpt    2  1365082.289          ops/s
SerializationBenchmark.identifiedDataSerializable  thrpt    2  1693094.657          ops/s
SerializationBenchmark.stream                      thrpt    2  1798280.394          ops/s
```

The very same object instantiated with sample data will also be encoded
with different number of bytes depending on used strategy:

```text
Strategy                                                   Number of Bytes   Overhead %
java.io.Serializable                                                   162          523
java.io.Externalizable                                                  87          234
com.hazelcast.nio.serialization.Portable                               104          300
com.hazelcast.nio.serialization.DataSerializable                        88          238
com.hazelcast.nio.serialization.IdentifiedDataSerializable              35           34
com.hazelcast.nio.serialization.StreamSerializer                        26            0
```

For more details on (de)serialization topic, you can refer to
[Hazelcast IMDG](https://docs.hazelcast.org/docs/4.0/manual/html-single/index.html#serialization).