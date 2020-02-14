---
title: Cluster Topology
id: cluster-topology
---

Hazelcast Jet is a distributed system, where each Hazelcast Jet node is
referred to as a _member_. Each member is a Java application, running in
a JVM.

Broadly speaking, Hazelcast Jet can be used in one of two ways:

1. **Dedicated**: Hazelcast Jet runs on a dedicated cluster,
   which is accessed by using the Jet Client that is available as a
   command line tool or through its Java API.
2. **Embedded**: Hazelcast Jet cluster member runs together with another
   JVM-based application, inside the same JVM. The nodes can be directly
   access through the Java API, without requiring the use of the Client
   API.

![Embedded vs Client-Server Cluster Topology](assets/deployment-options.png)

The functionality offered in both modes is similar, but there are
major differences in terms of deployment and operations.

## Dedicated Cluster

The standard way to deploy Hazelcast Jet is to run it in a dedicated
cluster. The cluster is deployed in one of the many
supported ways (using bare instances, Docker, Kubernets, etc) and the
clients interact with the cluster using the Hazelcast Jet Client API. A
single cluster is able to scale to several concurrenct jobs, from
hundreds to thousands depending on the pipelines.

In this mode, clients are able to deploy jobs along with the classes
required for job execution using either the `jet submit` command or the
Jet Client APIs. To create a Jet client, you use the snippet below:

```java
JetInstance client = Jet.newJetClient();
```

`JetInstance` type refers to both a client instance, or an embedded node
and all of the functionality is available in both APIs. The client API
may be seen as a proxy which sends the requested operations to the
server.

A Jet node generally tries to consume as much resources on a node as
possible to maximize performance. Having a dedicated Jet cluster
provides the full benefits of isolation and decouples the cluster from
the applications that are using it. There aren't any benefits to running
more than one Jet node in the same system since Jet automatically scales
to make use of all CPU cores.

Having a dedicated Jet cluster means you can also scale it independently
of its users, as Jet supports automatic scaling of streaming jobs.

In this mode, Jet typically wouldn't know about user-supplied classes so
any user code needs to be explicitly submitted with the Job. It is
recommended that heavyweight dependencies are also added to the
cluster's classpath so that they're not re-submitted with each job.

## Embedded Jet

Hazelcast Jet cluster member can be embedded and run inside the same JVM
with any other JVM based application. The code snipped below is all that is 
needed to create a Jet node:

```java
JetInstance jet = Jet.newJetInstance();
```

The main use of using Jet this way is for testing and development, as
you can easily create multiple Jet nodes and form a full cluster inside
a single JVM from an IDE for development purposes, without having to set
up complex infrastructure or dependencies. Each Jet node has a full Jet
execution engine which is not crippled or simulated in any way compared
to a dedicated cluster. This is one of the main benefits of Jet compared
to other simliar systems.

In this mode, the user doesn't need to worry about adding required
classes to the cluster, since everything is run as part of a single
application so the deployment model is very simple.

The host application can call directly into the Jet node without using
the client API. External applications still can interact with the
embedded Jet nodes using the Client API.

For production, this is only recommended in specific cases where you
would benefit from tightly coupling the cluster lifecycle to the
application. As mentioned earlier, Jet by default tries to consume all
resources to maximize computational throughput. Using Jet in this mode
together with a CPU and memory intensive application requires special
care to make sure that they do not interfere with each other.

If you're deploying Jet in a restricted or limited runtime environment
where you don't have full control of the host processes (such as in an
app server) then this mode might be suitable.

If you're interested in OEM'in Jet as part of another service, using
this mode is also a good choice (but with the above caveats regarding
performance).

## Summary

Use dedicated mode for:

* Mixed workloads that can scale independenly of the client applications
* Getting the best throughput for performance critical applications

Use embedded mode for:

* Development and testing
* Running on application servers or on restricted environments
* OEMs
* Simple, non-distributed pipelines (i.e. single node)