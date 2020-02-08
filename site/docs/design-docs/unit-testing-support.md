---
title: Pipeline API: Unit Testing Support
id: unit-testing
---

## Background

We want to improve the developer experience when you just want to have 
some data to mock a pipeline with. Currently this requires using one of
the built-in sources (file, sink, map etc) which, while somewhat 
convenient for batch they are quite inconvenient for streaming and 
are not completely foolproof. We also want to improve the experience
so that you can write tests to assert the output of a pipeline without
having to write boilerplate code (i.e. writing the results to an IList and then retrieve it to verify it later).
 
The functionality is implemented as two parts:

## Test Sources

There are now new sources which can be used to generate some data for 
writing a test pipeline:

### Batch Sources

These sources create a fixed amount of data. These sources are non-distributed.

```java 
BatchStage<Integer> source = pipeline.drawFrom(TestSources.items(1, 2, 3, 4));
```

```java 
List<Integer> list = new ArrayList<>();
list.add(1);
list.add(2);
 
BatchStage<Integer> source = pipeline.drawFrom(TestSources.items(list));
```

### Streaming Sources

Streaming sources create an infinite stream of data. The generated events have 
timestamps and like the batch source, this source is also non-distributed.

```java 
int itemsPerSecond = 10;
pipeline.drawFrom(TestSources.itemStream(itemsPerSecond))
        .withNativeTimestamp(0);
```

The source above will emit data as follows:

``` 
12:19:59,757  INFO || - [loggerSink#0] hz._hzInstance_2_jet.jet.blocking.ProcessorTasklet{loggerSink#0} - [127.0.0.1]:5702 [jet] [3.2-SNAPSHOT] (timestamp=12:19:59.700, sequence=0)
12:19:59,850  INFO || - [loggerSink#0] hz._hzInstance_2_jet.jet.blocking.ProcessorTasklet{loggerSink#0} - [127.0.0.1]:5702 [jet] [3.2-SNAPSHOT] (timestamp=12:19:59.800, sequence=1)
12:19:59,955  INFO || - [loggerSink#0] hz._hzInstance_2_jet.jet.blocking.ProcessorTasklet{loggerSink#0} - [127.0.0.1]:5702 [jet] [3.2-SNAPSHOT] (timestamp=12:19:59.900, sequence=2)
12:20:00,058  INFO || - [loggerSink#0] hz._hzInstance_2_jet.jet.blocking.ProcessorTasklet{loggerSink#0} - [127.0.0.1]:5702 [jet] [3.2-SNAPSHOT] (timestamp=12:20:00.000, sequence=3)
12:20:00,157  INFO || - [loggerSink#0] hz._hzInstance_2_jet.jet.blocking.ProcessorTasklet{loggerSink#0} - [127.0.0.1]:5702 [jet] [3.2-SNAPSHOT] (timestamp=12:20:00.100, sequence=4)
12:20:00,250  INFO || - [loggerSink#0] hz._hzInstance_2_jet.jet.blocking.ProcessorTasklet{loggerSink#0} - [127.0.0.1]:5702 [jet] [3.2-SNAPSHOT] (timestamp=12:20:00.200, sequence=5)
12:20:00,349  INFO || - [loggerSink#0] hz._hzInstance_2_jet.jet.blocking.ProcessorTasklet{loggerSink#0} - [127.0.0.1]:5702 [jet] [3.2-SNAPSHOT] (timestamp=12:20:00.300, sequence=6)
```

Each item has a sequence number and a timestamp, which can also be used to map
 to the desired item type:

```java 
int itemsPerSecond = 10;
 
pipeline.drawFrom(TestSources.itemStream(itemsPerSecond, (timestamp, sequence) -> new Trade(timestamp, ..)
```

## Assertions
Currently to make sure that a pipeline is outputting things correctly programmatically you have to write the results to either an IMap or IList and then assert the values there. This can be tedious and error-prone as for example IList doesn't implement equals(). You also have to move the assertion itself out of the pipeline, which is not always desired.

Several new sinks have been added to support asserting directly in the pipeline. Further, there's additional convenience to have the assertions done inline with the sink without having to terminate the pipeline, using the new apply operator. This is the preferred usage.

### Batch Assertions

Batch assertions collect all incoming items, and perform assertions on the collected list after all the items are received. If the assertion passes, then no exception is thrown. If the assertion fails, then the job will fail with an AssertionError.

#### Ordered Assertion
This asserts that items have been received in a certain order and no other items have been received. Only applicable to batch jobs.

```java 
pipline.drawFrom(TestSources.items(1, 2, 3, 4))
       .apply(Assertions.assertOrdered("unexpected values", Arrays.asList(1, 2, 3, 4)))
       .drainTo(Sinks.logger())
```

#### Unordered Assertion
Asserts that items have been received in any order and no other items have been received. Only applicable to batch stages.

```java 
pipeline.drawFrom(TestSources.items(4, 3, 2, 1)
 .apply(Assertions.assertAnyOrder("unexpected values", Arrays.asList(1, 2, 3, 4)))
 .drainTo(Sinks.logger())
```

#### Contains Assertions

Assert that the given items have been received in any order; receiving other, unrelated items does not affect this assertion. Only applicable to batch stages.

```java 
pipeline.drawFrom(TestSources.items(4, 3, 2, 1))
        .apply(Assertions.assertContains(Arrays.asList(1, 3)))
        .drainTo(Sinks.logger())
```

#### Collected Assertion

This is a more flexible assertion which is only responsible for collecting the received items, and passes the asserting responsibility to the user. It is a building block for the other assertions. Only applicable to batch stages.

```java 
pipeline.drawFrom(TestSources.items(1, 2, 3, 4))
        .apply(Assertions.assertCollected(items -> assertTrue("expected minimum of 4 items", items.size >= 4)))
        .drainTo(Sinks.logger())
```     

### Streaming Assertions
For streaming assertions, it's not possible to assert after all items have been received, as the stream never terminates. Instead, we periodically assert and throw if the assertion is not valid after a given period of time. However even if the assertion passes, we don't want to the job to continue running forever. Instead a special exception `AssertionCompletedException` is thrown to signal the assertion has passed successfully.

#### Collected Eventually Assertion
This assertion collects incoming items and runs the given assertion function repeatedly on the received item set. If the assertion passes at any point, the job will be completed with an AssertionCompletedException. If the assertion fails after the given timeout period, the job will fail with an `AssertionError`.

```java
pipeline.drawFrom(TestSources.itemStream(10))
 .withoutTimestamps()
 .apply(assertCollectedEventually(5, c -> assertTrue("did not receive at least 20 items", c.size() > 20)))
```   

The pipeline above with fail with an `AssertionError` if 20 items are not received after 5 seconds. The job will complete with an `AssertionCompletedException` as soon as 20 items or more are received.

### Assertion Sink Builder
Both the batch and streaming assertions use an assertion sink builder for building the assertions. Although a lower-level API, this is also public and can be used to build other, more complex assertions if desired:

```java 
/**
 * Returns a builder object that offers a step-by-step fluent API to build
 * an assertion {@link Sink} for the Pipeline API. An assertion sink is
 * typically used for testing of pipelines where you can want to run
 * an assertion either on each item as they arrive, or when all items have been
 * received.
 * <p>
 * These are the callback functions you can provide to implement the sink's
 * behavior:
 * <ol><li>
 *     {@code createFn} creates the state which can be used to hold incoming
 *     items.
 * </li><li>
 *     {@code receiveFn} gets notified of each item the sink receives
 *     and can either assert the item directly or add it to the state
 *     object.
 * </li><li>
 *     {@code timerFn} is run periodically even when there are no items
 *     received. This can be used to assert that certain assertions have
 *     been reached within a specific period in streaming pipelines.
 * </li><li>
 *     {@code completeFn} is run after all the items have been received.
 *     This typically only applies only for batch jobs, in a streaming
 *     job this method may never be called.
 * </li></ol>
 * The returned sink will have a global parallelism of 1: all items will be
 * sent to the same instance of the sink.
 *
 * It doesn't participate in the fault-tolerance protocol,
 * which means you can't remember across a job restart which items you
 * already received. The sink will still receive each item at least once,
 * thus complying with the <em>at-least-once</em> processing guarantee. If
 * the sink is idempotent (suppresses duplicate items), it will also be
 * compatible with the <em>exactly-once</em> guarantee.
 *
 * @param <A> type of the state object
 *
 * @since 3.2
 */
@Nonnull
public static <A> AssertionSinkBuilder<A, Void> assertionSink(
        @Nonnull String name,
        @Nonnull SupplierEx<? extends A> createFn
) {
  ..
}
```



## Future Improvements
In the future we may also consider adding fault tolerant sources to test fault tolerance. This would require a more complex source which is able to replay already emitted messages (using exact timestamp and sequence).








