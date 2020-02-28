---
title: Testing
---

Hazelcast Jet project uses JUnit testing framework to test
itself in various scenarios. Over the years there has been some
repetition within the test implementations and it led us to come up
with our own set of convenience methods for testing.

Hazelcast Jet provides test support classes to verify correctness of
your pipelines. Those support classes and the test sources are published
 to the Maven central repository for each version with the `tests`
classifier.

To start using them, please add following dependencies to your
project:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
compile 'com.hazelcast.jet:hazelcast-jet-core:4.0:tests'
compile 'com.hazelcast:hazelcast:4.0:tests'
```

<!--Maven-->

```xml
<dependency>
  <groupId>com.hazelcast.jet</groupId>
  <artifactId>hazelcast-jet-core</artifactId>
  <version>4.0</version>
  <classifier>tests</classifier>
</dependency>
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast</artifactId>
  <version>4.0</version>
  <classifier>tests</classifier>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

After adding the dependencies to your project, the test classes should
be available to your project.

## Creating Hazelcast Jet instances with Mock Network

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

## Integration Tests

For integration testing, there might be a need to create real instances
without the mock network. For those cases one can create real instances
with `Jet.newJetInstance()` method.

## Using random cluster names

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
        JetInstance[] instances = createJetMembers(jetConfig, 2);

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

## Cleaning up the resources

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

## Test Sources and Sinks

Hazelcast Jet comes with batch and streaming test sources along with a
assertion sinks where you can write tests to assert the output of a
pipeline without having to write boilerplate code.

Test sources allow you to generate events for your pipeline.

### Batch Source

These sources create a fixed amount of data. These sources are
non-distributed.

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.items(1, 2, 3, 4))
 .writeTo(Sinks.logger());
```

This will yield an output like below:

```log
10:25:50,198  INFO |batchSource| - [loggerSink#0] hz.cocky_lichterman.jet.blocking.thread-1 - [127.0.0.1]:5701 [jet] [4.0-SNAPSHOT] 1
10:25:50,199  INFO |batchSource| - [loggerSink#0] hz.cocky_lichterman.jet.blocking.thread-1 - [127.0.0.1]:5701 [jet] [4.0-SNAPSHOT] 2
10:25:50,200  INFO |batchSource| - [loggerSink#0] hz.cocky_lichterman.jet.blocking.thread-1 - [127.0.0.1]:5701 [jet] [4.0-SNAPSHOT] 3
10:25:50,200  INFO |batchSource| - [loggerSink#0] hz.cocky_lichterman.jet.blocking.thread-1 - [127.0.0.1]:5701 [jet] [4.0-SNAPSHOT] 4
```

### Streaming Source

Streaming sources create an infinite stream of data. The generated
events have timestamps and like the batch source, this source is also
non-distributed.

```java
int itemsPerSecond = 2;
pipeline.readFrom(TestSources.itemStream(itemsPerSecond))
        .withNativeTimestamp(0)
        .writeTo();
```

The source above will emit data as follows:

```log
10:28:27,654  INFO |streamingSource| - [loggerSink#0] hz.competent_margulis.jet.blocking.thread-1 - [127.0.0.1]:5701 [jet] [4.0-SNAPSHOT] (timestamp=10:28:27.500, sequence=0)
10:28:28,146  INFO |streamingSource| - [loggerSink#0] hz.competent_margulis.jet.blocking.thread-1 - [127.0.0.1]:5701 [jet] [4.0-SNAPSHOT] (timestamp=10:28:28.000, sequence=1)
10:28:28,647  INFO |streamingSource| - [loggerSink#0] hz.competent_margulis.jet.blocking.thread-1 - [127.0.0.1]:5701 [jet] [4.0-SNAPSHOT] (timestamp=10:28:28.500, sequence=2)
10:28:29,145  INFO |streamingSource| - [loggerSink#0] hz.competent_margulis.jet.blocking.thread-1 - [127.0.0.1]:5701 [jet] [4.0-SNAPSHOT] (timestamp=10:28:29.000, sequence=3)
10:28:29,648  INFO |streamingSource| - [loggerSink#0] hz.competent_margulis.jet.blocking.thread-1 - [127.0.0.1]:5701 [jet] [4.0-SNAPSHOT] (timestamp=10:28:29.500, sequence=4)
10:28:30,147  INFO |streamingSource| - [loggerSink#0] hz.competent_margulis.jet.blocking.thread-1 - [127.0.0.1]:5701 [jet] [4.0-SNAPSHOT] (timestamp=10:28:30.000, sequence=5)
```

For more detailed information regarding test sources and sinks
please see the [design document](design-docs/unit-testing-support.md).

## Assertions

`com.hazelcast.jet.core.JetTestSupport` contains a lot of assertion
methods which can be used to verify whether the job/member/cluster is
in desired state.

## Class Runners

There are multiple JUnit test class runners shipped with the tests
package which gives various abilities.

The common features are:

- Ability to print a thread dump in case of a test failure, configured
 via `hazelcast.test.threadDumpOnFailure` property
- Supports repetitive test execution
- Uses mock networking, unless configured to use real networking via `hazelscast.test.use.network`
 property
- Disabled phone-home feature, configured via `hazelcast.phone.home.enabled`
 property
- Have shorter(1 sec) wait time before joining than default(5 secs).
 This leads to faster cluster formation and test execution, configured
 via `hazelcast.wait.seconds.before.join` property.
- Uses loopback address, configured via `hazelcast.local.localAddress`
 property
- Uses IPv4 stack, configured via `java.net.preferIPv4Stack`
 property
- Prints out test execution duration after they finish execution

Let's have a look at them in detail:

### Serial Class Runner

`com.hazelcast.test.HazelcastSerialClassRunner` is a JUnit test class
runner which runs the tests in series. Nothing fancy, it just executes
the tests with the features listed above.

### Parallel Class Runner

`com.hazelcast.test.HazelcastParallelClassRunner` is a JUnit test class
runner which runs the tests in parallel with multiple threads. If the
test methods within the test class does not share any resources this
yields a faster execution compared to it's serial counterpart.

### Repetitive Test Execution

While dealing with intermittently failing tests, it is helpful to run
the test multiple times in series to increase chances to make it
fail. In those cases `com.hazelcast.test.annotation.Repeat` annotation
can be used to run the test repeatadly. `@Repeat` annotation can be
used on both the class and method level. On the class level it repeats the
whole class execution specified tiems. On the method level it only
repeats particular test method.

Follwing is an example test which repeats the test method execution
5 times:

```java
@RunWith(HazelcastSerialClassRunner.class)
public class RepetitiveTest extends JetTestSupport {

    @Repeat(5)
    @Test
    public void test() {
        System.out.println("Test method to be implemented!");
    }
}
```

When run, it logs like the following:

```log
Started Running Test: test
---> Repeating test [RepetitiveTest:test], run count [1]
Test method to be implemented!
---> Repeating test [RepetitiveTest:test], run count [2]
Test method to be implemented!
---> Repeating test [RepetitiveTest:test], run count [3]
Test method to be implemented!
---> Repeating test [RepetitiveTest:test], run count [4]
Test method to be implemented!
---> Repeating test [RepetitiveTest:test], run count [5]
Test method to be implemented!
Finished Running Test: test in 0.009 seconds.
```

> Note: `@Repeat` annotation only works with Hazelcast Class runners.

## Waiting for job to be in desired state

On some use cases, one needs to make the job is submitted and running
on the cluster before generating any events on the controlled source
to observe results. To achieve that following assertion could be used
to validate job is in the desired state.

```java

public class DesiredStateTest extends JetTestSupport {

    @Test
    public void given_singeNodeJet_when_jobIsRunning__then...() {
        // given
        JetInstance jet = createJetMember();

        // when
        Pipeline p = buildPipeline();
        Job job = jet.newJob(p);
        assertJobStatusEventually(job, JobStatus.RUNNING);

        // then
        ...
        ...
    }
...
}
```

In the example above `assertJobStatusEventually(Job, JobStatus)`
utility method has been used to validate the job is in the desired
state from the `com.hazelcast.jet.core.JetTestSupport` class.
