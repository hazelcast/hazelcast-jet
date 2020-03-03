---
title: Monitoring and Metrics
description: Options available for monitoring the health and operations of Jet clusters and jobs.
---

## Logging

Jet does not depend on a specific logging framework and has built-in
adapters for a variety of logging frameworks. You can also write a new
adapter to integrate with loggers Jet does not support natively. To use
one of the built-in adapters, set the `hazelcast.logging.type` property
to one of the following:

* `jdk`: java.util.logging (default)
* `log4j`: Apache Log4j
* `log4j2`: Apache Log4j 2
* `slf4j`: SLF4J
* `none`: Turn off logging

For example, to configure Jet to use Log4j, you can do one of the
following:

```java
System.setProperty("hazelcast.logging.type", "log4j");
```

or

```java
JetConfig config = new JetConfig();
config.getHazelcastConfig()
      .setProperty("hazelcast.logging.type", "log4j");
```

## Metrics

Jet exposes various metrics to facilitate monitoring of the cluster
state and of running jobs.

Metrics have associated **tags** which describe which object the metric
applies to. The tags for job metrics typically indicate the specific
[DAG vertex](../concepts/dag.md) and processor the metric belongs to.

Each metric instance provided belongs to a particular Jet cluster
member, so different cluster members can have their own versions of the
same metric with different values.

The metric collection runs in regular intervals on each member, but note
that the metric collection on different cluster members happens at
different moments in time. So if you try to correlate metrics from
different members, they can be from different moments of time.

There are two broad categories of Jet metrics. For clarity we will group
them based on significant tags which define their granularity.

Last but not least let’s not forget about the fact that each Jet member
is also a [Hazelcast](https://github.com/hazelcast/hazelcast) member, so
Jet also exposes all the metrics available in Hazelcast too.

Let’s look at these 3 broad categories of metrics in detail.

### Hazelcast Metrics

There is a wide range of metrics and statistics provided by Hazelcast:

* statistics of distributed data structures (see [Reference Manual](https://docs.hazelcast.org/docs/4.0/manual/html-single/index.html#getting-member-statistics))
* executor statistics (see [Reference Manual](https://docs.hazelcast.org/docs/4.0/manual/html-single/index.html#executor-statistics))
* partition related statistics (state, migration, replication)
* garbage collection statistics
* memory statistics for the JVM which current IMDG member belongs to
  (total physical/free OS memory, max/committed/used/free heap memory
  and max/committed/used/free native memory)
* network traffic related statistics (traffic and queue sizes)
* class loading related statistics
* thread count information (current, peak and daemon thread counts)

### Cluster-wide Metrics

<style type="text/css">
.tg  {border-collapse:collapse;border-spacing:0;border-color:#93a1a1;}
.tg td{font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;
border-style:solid;border-width:1px;overflow:hidden;word-break:normal;
border-color:#93a1a1;color:#002b36;background-color:#fdf6e3;}
.tg th{font-family:Arial, sans-serif;font-size:14px;font-weight:normal;
padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;
word-break:normal;border-color:#93a1a1;color:#fdf6e3;
background-color:#657b83;}
.tg .tg-2iel{border-color:#333333;text-align:left;vertical-align:top;
position:sticky;position:-webkit-sticky;top:-1px;will-change:transform}
.tg .tg-de2y{border-color:#333333;text-align:left;vertical-align:top}
</style>
<table class="tg">
  <tr>
    <th class="tg-2iel">Names</th>
    <th class="tg-2iel">Extra tags</th>
  </tr>
  <tr>
    <td class="tg-de2y" width="70%">
    <b>blockingWorkerCount</b>: Number of non-cooperative workers
       employed.<br>
    <br>
    <b>jobs.submitted</b>: Number of computational jobs submitted.<br>
    <b>jobs.completedSuccessfully</b>: Number of computational jobs
       successfully completed.<br>
    <b>jobs.completedWithFailure</b>: Number of computational jobs
       that have failed.<br>
    <b>jobs.executionStarted</b>: Number of computational job
       executions started. Each job can execute multiple times, for
       example when it’s restarted or suspended and then resumed.<br>
    <b>jobs.executionTerminated</b>: Number of computational job
       executions finished. Each job can execute multiple times, for
       example when it’s restarted or suspended and then resumed.<br>
    </td>
    <td class="tg-de2y">
    <b>none</b><br>
    Each Jet cluster member will have one instance of this metric.
    </td>
  </tr>
  <tr>
    <td class="tg-de2y">
    <b>iterationCount</b>: The total number of iterations the driver of
       tasklets in cooperative thread N made. It should increase by at
       least 250 iterations/s. Lower value means some of the cooperative
       processors blocks for too long. Somewhat lower value is normal if
       there are many tasklets assigned to the processor. Lower value
       affects the latency.<br>
    <b>taskletCount</b>: The number of assigned tasklets to cooperative
       thread N.
    </td>
    <td class="tg-de2y">
    <b>cooperativeWorker=&lt;N&gt;</b><br>
    N is the number of the cooperative thread.
    </td>
  </tr>
</table>

### Job-specific Metrics

All job specific metrics have their `job=<jobId>` and
`exec=<executionId>` tags set and most also have the
`vertex=<vertexName>` tag set (with very few exceptions). This means
that most of these metrics will have at least one instance for each
vertex of each current job execution.

Additionally, if the vertex sourcing them is a data source or data sink,
then the `source` or `sink` tags will also be set to true.

<style type="text/css">
.tg  {border-collapse:collapse;border-spacing:0;border-color:#93a1a1;}
.tg td{font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;
border-style:solid;border-width:1px;overflow:hidden;word-break:normal;
border-color:#93a1a1;color:#002b36;background-color:#fdf6e3;}
.tg th{font-family:Arial, sans-serif;font-size:14px;font-weight:normal;
padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;
word-break:normal;border-color:#93a1a1;color:#fdf6e3;
background-color:#657b83;}
.tg .tg-2iel{border-color:#333333;text-align:left;vertical-align:top;
position:sticky;position:-webkit-sticky;top:-1px;will-change:transform}
.tg .tg-de2y{border-color:#333333;text-align:left;vertical-align:top}
</style>
<table class="tg">
  <tr>
    <th class="tg-2iel">Names</th>
    <th class="tg-2iel">Extra tags</th>
  </tr>
  <tr>
    <td class="tg-de2y" width="60%">
    <b>executionStartTime</b>: Start time of the current execution of
       the job.<br>
    <b>executionCompletionTime</b>: Completion time of the current
       execution of the job.<br>
    <br>
    Both contain epoch time in milliseconds.
    </td>
    <td class="tg-de2y">
    <b>no extra tags, they are even missing <i>vertex</i></b><br>
    There will be a single instance of these metrics for each job execution.
    </td>
  </tr>
  <tr>
    <td class="tg-de2y">
    <b>snapshotBytes</b>: Total number of bytes written out in the last
       snapshot.<br>
    <b>snapshotKeys</b>: Total number of keys written out in the last
       snapshot.
    </td>
    <td class="tg-de2y">
    <b>no extra tags</b><br>
    There will be a single instance of these metrics for each vertex.
    </td>
  </tr>
  <tr>
    <td class="tg-de2y">
    <b>distributedBytesIn</b>: Total number of bytes received from
       remote members.<br>
    <b>distributedBytesOut</b>: Total number of bytes sent to remote
       members.<br>
    <b>distributedItemsIn</b>: Total number of items received from
       remote members.<br>
    <b>distributedItemsOut</b>: Total number of items sent to
     remote
       members.<br>
    <br>
    These values are only present for distributed edges, they only
    account for data actually transmitted over the network between
    members. This numbers include watermarks, snapshot barriers etc.
    </td>
    <td class="tg-de2y">
    <b>ordinal=&lt;N&gt;</b><br>
    Each Jet member will have an instance of these metrics for each
    ordinal of each vertex of each job execution.
    </td>
  </tr>
  <tr>
    <td class="tg-de2y">
    <b>topObservedWm</b>: This value is equal to the highest coalescedWm
       on any input edge of this processor.<br>
    <b>coalescedWm</b>: The highest watermark received from all inputs
       that was sent to the processor to handle.<br>
    <b>lastForwardedWm</b>: Last watermark emitted by the processor to
       output.<br>
    <b>lastForwardedWmLatency</b>: The difference between
       <i>lastForwardedWn</i> and the system time at the moment when
       metrics were collected.<br>
    <br>
    <b>queuesCapacity</b>: The total capacity of input queues.<br>
    <b>queuesSize</b>: The total number of items waiting in input
       queues.<br>
    <br>
    All input queues for all edges to the processor are summed in the
    above two metrics. If size is close to capacity, backpressure is
    applied and this processor is a bottleneck. Only input edges with
    equal priority are summed. If the processor has input edges with
    different priority, only edges with the highest priority will be
    reflected, after those are exhausted edges with the next lower
    priority will be reflected and so on.
    </td>
    <td class="tg-de2y">
    <b>proc=&lt;N&gt;, ordinal=&lt;not specified&gt;</b><br>
    Each Jet member will have one instances of these metrics for each
    processor instance N, the N denotes the global processor index.
    Processor is the parallel worker doing the work of the vertex.
    </td>
  </tr>
  <tr>
    <td class="tg-de2y">
    <b>topObservedWm</b>: The highest received watermark from any
       input on edge N.<br>
    <b>coalescedWm</b>: The highest watermark received from all upstream
       processors on edge N.<br>
    <br>
    <b>emittedCount</b>: The number of emitted items. This number
    includes watermarks, snapshot barriers etc. Unlike
    <i>distributedItemsOut</i>, it includes items emitted items to local
    processors.<br>
    <b>receivedCount</b>: The number of received items. This number does
    not include watermarks, snapshot barriers etc. It’s the number of
    items the Processor.process method will receive.<br>
    <b>receivedBatches</b>: The number of received batches.
    <code>Processor.process</code> receives a batch of items at a time,
    this is the number of such batches. By dividing <i>receivedCount</i>
    by <i>receivedBatches</i>, you get the average batch size. It will
    be 1 under low load.
    </td>
    <td class="tg-de2y">
    <b>proc=&lt;N&gt;, ordinal=&lt;M&gt;</b><br>
    Each Jet member will have one instance of these metrics for each
    edge M (input or output) of each processor N. N is the global
    processor index and M is either the ordinal of the edge or has the
    value snapshot for output items written to state snapshot.
    </td>
  </tr>
  <tr>
    <td class="tg-de2y">
    <b>numInFlightOps</b>: The number of pending (in flight) operations
       when using asynchronous mapping processors. See
       <a href="https://docs.hazelcast.org/docs/jet/latest-dev/javadoc/com/hazelcast/jet/core/processor/Processors.html#mapUsingServiceAsyncP-com.hazelcast.jet.pipeline.ServiceFactory-int-boolean-com.hazelcast.function.FunctionEx-com.hazelcast.function.BiFunctionEx-">
       Processors.mapUsingServiceAsyncP
       </a>.<br>
    <br>
    <b>totalKeys</b>: The number of active keys being tracked by a
       session window processor.<br>
    <b>totalWindows</b>: The number of active windows being tracked by a
       session window processor. See
       <a href="https://docs.hazelcast.org/docs/jet/latest-dev/javadoc/com/hazelcast/jet/core/processor/Processors.html#aggregateToSessionWindowP-long-long-java.util.List-java.util.List-com.hazelcast.jet.aggregate.AggregateOperation-com.hazelcast.jet.core.function.KeyedWindowResultFunction-">
       Processors.aggregateToSessionWindowP</a>.<br>
    <br>
    <b>totalFrames</b>: The number of active frames being tracked by a
       sliding window processor.<br>
    <b>totalKeysInFrames</b>: The number of grouping keys associated
       with the current active frames of a sliding window processor.
       See
       <a href="https://docs.hazelcast.org/docs/jet/latest-dev/javadoc/com/hazelcast/jet/core/processor/Processors.html#aggregateToSlidingWindowP-java.util.List-java.util.List-com.hazelcast.jet.core.TimestampKind-com.hazelcast.jet.core.SlidingWindowPolicy-long-com.hazelcast.jet.aggregate.AggregateOperation-com.hazelcast.jet.core.function.KeyedWindowResultFunction-">
       Processors.aggregateToSlidingWindowP</a>.<br>
    <br>
    <b>lateEventsDropped</b>: The number of late events dropped by
       various processor, due to the watermark already having passed
       their windows.<br>
    </td>
    <td class="tg-de2y">
    <b>proc=&lt;N&gt;, procType=&lt;set&gt;</b><br>
    Processor specific metrics, only certain types of processors have
    them. The <i>procType</i> tag can be used to identify the exact type
    of processor sourcing them. Like all processor metrics, each Jet
    member will have one instances of these metrics for each processor
    instance N, the N denotes the global processor index.
    </td>
  </tr>
</table>

### User-defined Metrics

User-defined metrics are actually a subset of
[job metrics](#job-specific-metrics). What distinguishes them from
regular job-specific metrics is exactly what their name implies: they
are not built-in, but defined when processing pipelines are written.

Since user-defined metrics are also job metrics, they will have all the
tags job metrics have. They also have an extra tag, called `user` which
is of type `boolean` and is set to `true`.

> Due to the extra tag user-defined metrics have it’s not possible for
> them to overwrite a built-in metric, even if they have the exact
> same name.

Let’s see how one would go about defining such metrics. For example if
you would like to monitor your filtering step you could write code like
this:

```java
p.readFrom(source)
 .filter(l -> {
     boolean pass = l % 2 == 0;
     if (!pass) {
         Metrics.metric("dropped").increment();
     }
     Metrics.metric("total").increment();
     return pass;
 })
 .writeTo(sink);
```

For further details consult the [Javadoc](https://docs.hazelcast.org/docs/jet/latest-dev/javadoc/com/hazelcast/jet/core/metrics/Metrics.html).

User-defined metrics can be used anywhere in pipeline definitions where
custom code can be added. This means (just to name the most important
ones): filtering, mapping and flat-mapping functions, various
constituent functions of aggregations (accumulate, create, combine,
deduct, export & finish), key extraction function when grouping, in
[custom sources, sinks](../tutorials/source-and-sink-builder.md) and
processors and so on.

### Exposing Metrics

The main method Jet has for exposing the metrics to the outside world is
the JVM’s standard **JMX** interface. Since Jet 3.2 there is also an
alternative to JMX for monitoring metrics, via the Job API, albeit
only the job-specific ones.

#### Over JMX

Jet exposes all of its metrics using the JVM’s standard JMX interface.
You can use tools such as **Java Mission Control** or **JConsole** to
display them. All Jet-related beans are stored under
`com.hazelcast.jet/Metrics/<instanceName>/` node and the various tags
they have form further sub-nodes in the resulting tree structure.

Hazelcast metrics are stored under the
`com.hazelcast/Metrics/<instanceName>/` node.

#### Via Job API

The `Job` class has a
[`getMetrics()`](https://docs.hazelcast.org/docs/jet/latest-dev/javadoc/com/hazelcast/jet/Job.html#getMetrics--)
method which returns a
[JobMetrics](https://docs.hazelcast.org/docs/jet/latest-dev/javadoc/com/hazelcast/jet/core/metrics/JobMetrics.html)
instance. It contains the latest known metric values for the job.

This functionality has been developed primarily to give access to
metrics of finished jobs, but can in fact be used for jobs in any state.

For details on how to use and filter the metric values consult the
[JobMetrics API docs](https://docs.hazelcast.org/docs/jet/latest-dev/javadoc/com/hazelcast/jet/core/metrics/JobMetrics.html)
. A simple example for computing the number of data items emitted by a
certain vertex (let’s call it vertexA), excluding items emitted to the
snapshot, would look like this:

```java
Predicate<Measurement> vertexOfInterest =
        MeasurementPredicates.tagValueEquals(MetricTags.VERTEX, "vertexA");
Predicate<Measurement> notSnapshotEdge =
        MeasurementPredicates.tagValueEquals(MetricTags.ORDINAL, "snapshot").negate();

Collection<Measurement> measurements = jobMetrics
        .filter(vertexOfInterest.and(notSnapshotEdge))
        .get(MetricNames.EMITTED_COUNT);

long totalCount = measurements.stream().mapToLong(Measurement::value).sum();
```

### Configuration

The metrics collection is enabled by default. You can configure it using
the hazelcast-jet.yml file (see also
[full guide](../operations/configuration.md) on
configuration):

```yaml
metrics:
  enabled: true
  collection-frequency-seconds: 5
  jmx:
    enabled: true
```
