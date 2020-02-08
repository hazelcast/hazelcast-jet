---
title: Cluster Design
id: cluster-design
---

## Cluster Topologies

Hazelcast Jet cluster is formed by members. Each member is a Java
application.

Hazelcast Jet supports two modes of operation: embedded and
client-server. In an embedded deployment, the Hazelcast Jet cluster
member runs in-process with your application. Your JVM hosts both your
application and Jet cluster services and its data. You can use Java API
of Jet to control Jet cluster from the application code.

In a client-server deployment, Hazelcast Jet services are deployed to a
dedicated JVMs. The cluster is controlled through Jet client. Jet client
is available in Java and as a command-line tool.

These two topology approaches are illustrated in the following diagrams.

### Embedded Architecture

The main advantage of using embedded topology is simplicity. Because
Hazelcast Jet runs in the same JVMs as your application, there are no
extra servers to deploy, manage, or maintain. This applies especially
when the Hazelcast Jet cluster is tied directly to the embedded
application. Since Hazelcast Jet and the application are co-located,
reads and writes are faster because another network hop is not required
(as opposed to client-server topology).

When Hazelcast Jet is embedded in an application, it must be started and
shut down in concert with its co-resident application instance, and
vice-versa. In some cases this may lead to increased operational
complexity. Another thing to keep in mind that in embedded mode, your
application and Hazelcast Jet will share the same resources.

Use embedded architecture for:

* Microservices
* OEM
* Restricted runtime environments (edge deployments, strict operational
  restrictions) - one package such as JAR or Docker with all runtime
  dependencies
* Development Environment - simplicity

### Client-Server Architecture

The main advantage of client-server topology is isolation. The Hazelcast
Jet cluster will use its own resources instead of sharing it with an
application. Therefore, the client application and Hazelcast Jet cluster
can be turned on and off independently. Additionally, it’s easier to
identify problems if the Hazelcast Jet cluster and application are
isolated.

The practical lifecycle of Hazelcast Jet cluster members is usually
different from any particular application instance. When Hazelcast Jet
is embedded in an application instance, the embedded Hazelcast Jet
member will be started and shut down alongside its co-resident
application instance and vice-versa. When Hazelcast Jet members are
deployed as separate JVMs, they and their client application instances
may be started and shut down independently.

Hazelcast Jet processing is data driven. Jet tries to process as much
data as it can. Standalone cluster does not compete with the application
for CPU, memory and I/O resources. This makes the performance of both
your application and Hazelcast Jet more predictable, reducing the noisy
neighbourhood effect.

When Hazelcast Jet member activity is isolated to its own server, it’s
easier to identify the cause of any pathological behaviour. For example,
if there is a memory leak in the application causing unbounded heap
usage growth, the memory activity of the application is not obscured by
the co-resident memory activity of Hazelcast Jet services. The same
holds true for CPU and I/O issues. When application activity is isolated
from Hazelcast Jet services, symptoms are automatically isolated and
easier to recognise.

The client-server architecture is appropriate when using Hazelcast Jet
as a shared infrastructure used by multiple applications, especially
those under the control of different work groups.

The client-server architecture has a more flexible scaling profile. When
you need to scale, simply add more Hazelcast Jet servers. With the
client-server deployment model, client and server scalability concerns
may be addressed independently.

With Client-Server, Hazelcast Jet nodes run in dedicated JVMs. There are
extra costs to deploy, manage, or maintain extra servers.

Use Client-Server Architecture for:

* Shared Infrastructure - Jet hosts workloads from multiple applications
* Resource and problem isolation
* Separation of concerns
