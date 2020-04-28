---
title: Jet 4.1 is Released
author: Can Gencer
authorURL: http://twitter.com/cgencer
authorImageURL: https://pbs.twimg.com/profile_images/1187734846749196288/elqWdrPj_400x400.jpg
---

We are happy to present the new release of Jet 4.1. It brings the
following new features:

## Extended gRPC Support

We've applied the lessons learned from the Jet-Python integration and
made it easier now to integrate a Jet pipeline with
[gRPC](https://grpc.io) services. It's possible to use the new
`GrpcServices` class to create a `ServiceFactory` which can be used
in-conjunction with the `mapUsingServiceAsync` operator. Using this
integration can be a significant performance boost vs using the sync
`mapUsingService` call.

The integration can be used in the following way as part of a pipeline:

```java
var greeterService = unaryService(
    () -> ManagedChannelBuilder.forAddress("localhost", 5000).usePlaintext(),
    channel -> GreeterGrpc.newStub(channel)::sayHello
);

Pipeline p = Pipeline.create();
p.readFrom(TestSources.items("one", "two", "three", "four"))
 .mapUsingServiceAsync(greeterService, (service, input) -> {
    HelloRequest request = HelloRequest.newBuilder().setName(input).build();
    return service.call(request);
}).writeTo(Sinks.logger());
```

Additionally, bidirectional streaming and batching modes are also
supported. For a more in-depth look, please see the [Call a gRPC
Service how-to guide](/docs/how-tos/grpc) and the [design
document](/docs/design-docs/007-grpc-support).

## Transactional JDBC and JMS sinks

In Jet 4.0 we added support for [transactional sources and
sinks](/blog/2020/02/20/transactional-processors) through the use of
two-phase-commit. We're now extending this support for two additional
sinks: JDBC and JMS. The support requires the broker or the database to
support XA transactions. To test your database's support for XA
transactions, we've also released a [how-to guide](/docs/how-tos/xa).

You can also see a full summary of sinks and sources and the variety
of transaction support in the [sources and sinks page](/docs/api/sources-sinks#summary).

## Code Deployment Improvements

Previously Jet supported adding classes by name to
`JobConfig.addClass()`. It's not possible to add a whole package by
`JobConfig.addPackage()`. `addClass()` automatically will now add any
nested classes as well. You can see the [design
document](/docs/design-docs/001-code-deployment-improvements) for more
details.

## Job-scoped Serializer Support

It's now possible to register job-level serializers when submitting jobs
as opposed to global serializers which are registered during startup.

```java
JobConfig config = new JobConfig()
  .registerSerializer(Person.class, PersonSerializer.class);

jet.newJob(pipeline, config);
```

These serializers will only be used inside the job and to serialize any
items which need to sent between different nodes. You can read more about
how serialization in Hazelcast Jet works in the [serialization guide](/docs/api/serialization).

## Protocol Buffers Support

Another extension we've done with Serialization is to make it more
convenient to use [Protocol
Buffers](https://developers.google.com/protocol-buffers) for
serialization. It's possible to simply declare a serializer that uses
Protocol Buffers as follows:

```java
class PersonSerializer extends ProtobufSerializer<Person> {

    private static final int TYPE_ID = 1;

    PersonSerializer() {
        super(Person.class, TYPE_ID);
    }
}
```

For more information, see the [serialization guide](/docs/api/serialization#google-protocol-buffers).

## Spring Boot Starter

Spring Boot is a framework to create stand-alone Spring based
applications that just run. Spring Boot provides auto configuration of
some of the commonly used libraries through spring-boot-starters.
Hazelcast Jet now provides its own [Spring Boot
Starter](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/hazelcast-jet-spring-boot-starter).
which can be used to auto configure and start a Hazelcast Jet instance. 

Simply by adding the starter dependency to your Spring Boot application,
you can start a `JetInstance` with the default configuration. If you
want to customize the configuration all you need to do is to add a
configuration file (`hazelcast-jet.yaml`) to your class path or working
directory. The starter will pick that up and configure your Hazelcast
Jet instance.  If it is a client instance you want then you should add
the client configuration file (`hazelcast-client.yaml`) to your class
path or working directory.

For more details, see the [design document](docs/design-docs/004-spring-boot-starter).

## Kubernetes Operator and OpenShift Support

We've also released the first version of the [Hazelcast Jet Kubernetes
Operator](https://operatorhub.io/?keyword=jet). It's available for both
Hazelcast Jet open-source and enterprise. Hazelcast Jet Enterprise
Operator is also a certified by Red Hat and available on the Red Hat
Marketplace.

## Discovery Support for Microsoft Azure

We've added new convenience for Microsoft Azure. It's now possible to
configure Azure discovery as below:

```yaml
hazelcast:
  network:
    join:
      multicast:
        enabled: false
      azure:
        enabled: true
        tag: TAG-NAME=HZLCAST001
        hz-port: 5701-5703
```

For more details, please see the [discovery guide](/docs/operations/discovery#azure-cloud).

## Full Release Notes

Members of the open source community that appear in these release notes:

- @TomaszGaweda
- @caioguedes
- @SapnaDerajeRadhakrishna

Thank you for your valuable contributions!

### New Features

- [jms] Exactly-once guarantee for JMS sink (#1813)
- [jdbc] Exactly-once guarantee for JDBC sink (#1813)
- [core] JobConfig.addClass() automatically adds nested classes to the job (#1932)
- [core] JobConfig.addPackage() adds a whole Java package to the job (#1932, #2077)
- [core] Job-scoped serializer deployment (#2020, #2038, #2039, #2043, #2071, #2075, #2082, #2190)
- [core] [006] Protobuf serializer support (#2100)
- [pipeline-api] [007] Support gRPC for mapUsingService (#2095, #2185)

### Enhancements

- [jet-cli] Use log4j2 instead of log4j (#1981)
- [jet-cli] Simplify default log output (#2047)
- [core] Add useful error message when serializer not registered (#2061)
- [jet-cli] Add hazelcast-azure cluster self-discovery plugin to the
    fat JAR in the distribution archive (#2079)
- [pipeline-api] First-class support for inner hash join (@TomaszGaweda #2089)
- [core] When Jet starts up, it now logs the cluster name (@caioguedes #2105)
- [core] Add useful error message when trying to deploy a JDK class with JobConfig (#2108)
- [core] Implement JobConfig.toString (@SapnaDerajeRadhakrishna #2152)
- [core] Do not destroy Observable on shutdown (#2170)

### Fixes

- [core] Don't send the interrupt signal to blocking threads when a job is terminating (#1971)
- [core] Consistently prefer YAML over XML config files when both present (#2033)

### Breaking Changes

- [avro] Replace Supplier<Schema> with just Schema for Avro Sink (#2005)
- [jms] Reorder parameters in JMS source so the lambda comes last (#2062)
- [jet-cli] Change smart routing (connecting to all cluster members) default to disabled (#2104)
- [pipeline-api] For xUsingServiceAsync transforms, reduce the default number of concurrent
               service calls per processor. Before: 256; now: 4. (#2204)