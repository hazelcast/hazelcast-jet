---
title: Testing
id: testing
---

Hazelcast Jet project uses JUnit testing framework to test
itself in various scenarios. Over the years there has been some
repetition within the test implementations and it led us to come up
with our own set of convenience methods for testing.

Hazelcast Jet provides test support classes to verify correctness of
your pipelines. Those support classes and the test sources are published
 to the Maven central repository for each version with the `tests`
classifier.

To start using them, please add following dependency to your
project:

<!--DOCUSAURUS_CODE_TABS-->
<!--Maven-->

```xml
<dependencies>
  <dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet-core</artifactId>
    <version>4.0</version>
    <classifier>tests</classifier>
  </dependency>
</dependencies>
```

<!--Gradle-->

```bash
compile 'com.hazelcast.jet:hazelcast-jet-core:4.0:tests'
```

<!--END_DOCUSAURUS_CODE_TABS-->

After adding the dependency to your project, the test classes should be
available to your project.

== Creating Hazelcast Jet instances with Mock Network

Test utilities contains factory methods to create Hazelcast Jet
instances with the mock network stack. Those Hazelcast Jet nodes
communicate with each other using intra-process communication methods.
This means we can create multiple lightweight Hazelcast Jet instances
in our tests without using any networking resources.

To create Hazelcast Jet instances with mock network(along with a lot
of convenience methods), one needs to extend `com.hazelcast.jet.core.JetTestSupport`
class.

Here is a simple test harness which creates 2 node Hazelcast Jet cluster
with mock network:

```java
public class ClusteringTest extends JetTestSupport {

    @Test
    public void given_2nodeCluster_when...._then...() {
        // given
        JetInstance instance1 = createJetMember();
        JetInstance instance2 = createJetMember();
        // Alternatively
        // JetInstance[] instances = createJetMembers(2);

        // when
        ...
        ...
        // then
        ...
        ...
    }
...
}
```

If needed, a `JetConfig` object can be passed to the factory methods
like below:

```java
public class ClusteringTest extends JetTestSupport {

    @Test
    public void given_2nodeClusterWith16CooperativeThreads_when...._then...() {
        // given
        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(16);

        JetInstance[] instances = createJetMembers(config, 2);
        // when
        ...
        ...
        // then
        ...
        ...
    }
...
}
```

Similar to the Hazelcast Jet nodes, one can create Hazelcast Jet clients
with the same factories. There is no need to provide any network
configuration for clients to discover nodes since they are using the
same factory. The discovery will work out of the box.

```java
public class ClusteringTest extends JetTestSupport {

    @Test
    public void given_2nodeClusterAndClient_when...._then...() {
        // given
        JetInstance[] instances = createJetMembers(2);
        JetInstance client = createJetClient();
        // when
        ...
        ...
        // then
        ...
        ...
    }
...
}
```

When the above test run it should create 2 Hazelcast Jet nodes and a
Hazelcast Jet client connected to them. When run, it can be verified
that they form the cluster from the logs and client connected to them.

```log
10:45:03.927 [ INFO] [c.h.i.c.ClusterService]
....
Members {size:2, ver:2} [
 Member [127.0.0.1]:5701 - 93328d97-0975-4dfa-bf56-4d46e8a469a1 this
 Member [127.0.0.1]:5702 - 920d1b0c-0898-4b6e-9009-8f29889d6a77
]

10:45:03.933 [ INFO] [c.h.i.c.ClusterService]

Members {size:2, ver:2} [
 Member [127.0.0.1]:5701 - 93328d97-0975-4dfa-bf56-4d46e8a469a1
 Member [127.0.0.1]:5702 - 920d1b0c-0898-4b6e-9009-8f29889d6a77 this
]
....
10:45:04.890 [ INFO] [c.h.c.i.s.ClientClusterService] [4.0]

Members [2] {
 Member [127.0.0.1]:5701 - 93328d97-0975-4dfa-bf56-4d46e8a469a1
 Member [127.0.0.1]:5702 - 920d1b0c-0898-4b6e-9009-8f29889d6a77
}
```

First two blocks are the member list printed from each member's point
of view and the last one is the cluster from the client's point of view.

So far, we've seen how to create any number of Hazelcast Jet clusters
and clients using factory methods provided within the `com.hazelcast.jet.core.JetTestSupport`
class to create the test environments. Let's explore other utilities to
write meaningful test.

== Integration Tests

For integration testing, there might be a need the real instances
without the mock network. For those cases one can create real instances
with `Jet.newJetInstance()` method.

=== Using random cluster names

If multiple tests are running in parallel there is chance that the
clusters in each test can discover others, intefere the test
execution and most of the time causing both of them to fail.

To avoid such scenarios, one needs to isolate the clusters in each test
execution by giving them unique cluster names. This way, they won't
try to connect each other since the nodes will only try to connect to
other members with the same cluster name property.

Random cluster name for each test execution can be generated like
below:

```java

public class ClusteringTest extends JetTestSupport {

    @Test
    public void given_2nodeClusterAndClient_when..._then...() {
        // given
        String clusterName = randomName();
        JetConfig jetConfig = new JetConfig();
        jetConfig.getHazelcastConfig().setClusterName(clusterName);
        JetInstance[] instances = createJetMembers(2);

        JetClientConfig clientConfig = new JetClientConfig();
        clientConfig.setClusterName(clusterName);
        JetInstance client = createJetClient(clientConfig);
        // when
        ...
        ...
        // then
        ...
        ...
    }
...
}
```

In the example above `randomName()` utility method has been used to
generate a random string from `com.hazelcast.jet.core.JetTestSupport`
class.

=== Cleaning up the resources

Mock instances created from the factory of `com.hazelcast.jet.core.JetTestSupport`
are cleaned-up automatically after the test execution has been finished.

If the test contains real instances, then they either needs to be
tracked individually and shut down when the test finished or one can
write a teardown method like below to shut down all instances created.

```java
    @After
    public void after() {
        Jet.shutdownAll();
    }
```

Either way one has to shut down Hazelcast Jet instances after the test
has been finished to reclaim resources and not to leave a room for
interference with the next test execution due to distributed
nature of the product.

=== Assertions

TODO

=== Serial and Parallel Class Runners

TODO

=== Repetitive Test Execution

TODO

== Testing Hazelcast Jet Pipelines

TODO

=== Waiting for job to be in `RUNNING` state

TODO

=== Test Sources and Sinks

TODO
