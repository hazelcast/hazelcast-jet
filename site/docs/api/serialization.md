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
and knowing differences between each of supported strategies is 
crucial to efficient Jet usage.

Hazelcast Jet closely integrates with Hazelcast IMDG exposing many of 
its features to Jet users. In particular, one can use IMDG data 
structure as Jet `Source` and/or `Sink`. Objects retrieved from and 
stored in those have to be (de)serializable.

Another case which might require (de)serializable objects is sending 
computation results between remote vertices. Hazelcast Jet tries to 
minimize network traffic as much as possible, nonetheless different 
parts of a `DAG` can reside on separate cluster members. To catch 
(de)serialization issues early on, we recommend using a 2-member local 
Jet cluster for development and testing.

Currently, Hazelcast Jet offers 5 ways to (de)serialize objects:
- [java.io.Serializable](https://docs.oracle.com/javase/8/docs/api/java/io/Serializable.html)
- [java.io.Externalizable](https://docs.oracle.com/javase/8/docs/api/java/io/Externalizable.html)
- [com.hazelcast.nio.serialization.IdentifiedDataSerializable](https://docs.hazelcast.org/docs/latest/javadoc/com/hazelcast/nio/serialization/IdentifiedDataSerializable.html)
- [com.hazelcast.nio.serialization.Portable](https://docs.hazelcast.org/docs/latest/javadoc/com/hazelcast/nio/serialization/Portable.html)
- [com.hazelcast.nio.serialization.StreamSerializer](https://docs.hazelcast.org/docs/latest/javadoc/com/hazelcast/nio/serialization/StreamSerializer.html)

each having its cons and pros. You can refer to 
[Hazelcast IMDG](https://docs.hazelcast.org/docs/4.0/manual/html-single/index.html#serialization) 
for implementation details and in depth analysis. Here, for comparison 
sake we are just going to present some rough performance numbers one 
can expect when employing each of those strategies.

A straightforward benchmark which continuously serializes and then 
deserializes very simple object
```
class Person {
    private String firstName;
    private String lastName;
    private int age;
    private float height;
}
```

counting the total throughput, yields following results: 
```
# Processor: Intel(R) Core(TM) i7-4700HQ CPU @ 2.40GHz
# VM version: JDK 13, OpenJDK 64-Bit Server VM, 13+33

Benchmark                                 Mode  Cnt        Score   Error  Units
SerializationBenchmark.serializable      thrpt    2   179323.545          ops/s
SerializationBenchmark.externalizable    thrpt    2   334753.811          ops/s
SerializationBenchmark.dataSerializable  thrpt    2  1626386.988          ops/s
SerializationBenchmark.portable          thrpt    2  1073038.895          ops/s
SerializationBenchmark.streamSerializer  thrpt    2  2119776.478          ops/s
```