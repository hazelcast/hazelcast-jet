---
title: Custom Sources and Sinks
---

In the [Custom Sources and Sinks](../api/sources-sinks.md#custom-sources-and-sinks)
section of our [Sources and Sinks](../api/sources-sinks.md) programming
guide we have seen some basic examples of user-defined sources and
sinks. Let us now examine more examples which cover some of the
trickier aspects of writing our own sources and sinks.

## Sink

Let's write a sink that functions a bit like a **file logger**. You
set it up with a filename and it will write one line for each
input it gets into that file. The lines will be composed of a
**timestamp**, a floating point number providing us with the machine's
**CPU usage** at the time of writing and then the **`toString()`** form
of whatever input object produced the line.

One thing we'll need is code for obtaining CPU usage. We can use
[JavaSysMon](https://github.com/jezhumble/javasysmon), we'll need the
following imports in our project:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
compile 'com.danielflower.apprunner:javasysmon:0.3.5.1'
```

<!--Maven-->

```xml
<dependency>
  <groupId>com.danielflower.apprunner</groupId>
  <artifactId>javasysmon</artifactId>
  <version>0.3.5.1</version>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

The code we need can be something like this:

```java
class CpuMonitor {

    public static final long REFERENCE_REFRESH_PERIOD = TimeUnit.SECONDS.toMillis(1);

    private JavaSysMon sysMon = new JavaSysMon();
    private long prevTimeOrigin = System.currentTimeMillis();
    private CpuTimes prevTimes = sysMon.cpuTimes();

    float getCpuUsage() {
        CpuTimes cpuTimes = sysMon.cpuTimes();
        float usage = cpuTimes.getCpuUsage(prevTimes);
        updatePrevTimes(cpuTimes);
        return usage;
    }

    private void updatePrevTimes(CpuTimes cpuTimes) {
        long currentTime = System.currentTimeMillis();
        if (currentTime > prevTimeOrigin + REFERENCE_REFRESH_PERIOD) {
            prevTimeOrigin = currentTime;
            prevTimes = cpuTimes;
        }
    }
}

```

We will also need a **context** object to hold the entities we need in
our sink, the `CpuMonitor` and the file-based `PrintWriter`:

```java
class Context {

    private PrintWriter printWriter;
    private CpuMonitor cpuMonitor = new CpuMonitor();

    public Context(String fileName) {
        try {
            this.printWriter = new PrintWriter(new FileWriter(fileName));
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    void write(Object item) {
        String line = String.format("%d,%f,%s",
            System.currentTimeMillis(), cpuMonitor.getCpuUsage(), item.toString());
        printWriter.println(line);
        printWriter.flush();
    }

    void destroy() {
        printWriter.close();
        printWriter = null;

        cpuMonitor = null;
    }
}
```

Now that we have all the helper code ready we can write our actual sink:

```java
Sink<Object> cpuSink = sinkBuilder(
    "cpu-sink", x -> new Context("data.csv"))
    .receiveFn((ctx, item) -> ctx.write(item))
    .destroyFn(ctx -> ctx.destroy())
    .build();
```

That's it! All we did was to specify:

* how to set up our context object (the `createFn`)
* how to write out received object (the `receiveFn`)
* how to tear down the used resources once we are done (the `destroyFn`)

We can then run it in a dummy pipeline and use its output to
test the sources we are going to write in the following sections:

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.itemStream(10))
 .withoutTimestamps()
 .writeTo(cpuSink);

JetInstance jet = Jet.bootstrappedInstance();
jet.newJob(p).join();
```

### Batching

Our sink uses a `PrintWriter` which has internal buffering we could use
to make it more efficient. Jet allows us to make buffering a first-class
concern and deal with it explicitly by taking an optional `flushFn`
which it will call at regular intervals.

To apply this to our example we would make following changes:

```java
private static class Context {

    //...

    void write(Object item) {
        String line = String.format("%d,%f,%s", System.currentTimeMillis(),
                cpuMonitor.getCpuUsage(), item.toString());
        printWriter.println(line);
    }

    void flush() {
        printWriter.flush();
    }

    //...

}

Sink<Object> cpuSink = sinkBuilder(
    "file-sink", x -> new Context("data.csv"))
    .receiveFn((ctx, item) -> ctx.write(item))
    .flushFn(ctx -> ctx.flush())
    .destroyFn(ctx -> ctx.destroy())
    .build();

```

### Parallelism

Jet builds the sink to be distributed by default: each member of the Jet
cluster has a processor running it. You can configure how many parallel
processors there are on each member (the local parallelism) by calling
`SinkBuilder.preferredLocalParallelism()`. By default there will be one
processor per member.

Our sink, as presented here, will create the file `data.csv` on each
member, so the overall job output consists of the contents of all these
files put together.

```java
Sink<Object> cpuSink = sinkBuilder(
    "file-sink", x -> new Context("data.csv"))
    .receiveFn((ctx, item) -> ctx.write(item))
    .flushFn(ctx -> ctx.flush())
    .destroyFn(ctx -> ctx.destroy())
    .preferredLocalParallelism(2)
    .build();
```

>Note: if you run this sink with the dummy pipeline mentioned above you
> will NOT be able to observe these effects in action (you will not
> have multiple output files), because the `TestSource` used always
> has only a single instance and there will not be any partitioned
> edges in the DAG you end up with, so regardless how many sinks you
> set up, only one of them will get any data (and create an output
> file).

### Fault Tolerance

Sinks built via `SinkBuilder` don’t participate in the fault tolerance
protocol. You can’t preserve any internal state if a job fails and gets
restarted. In a job with snapshotting enabled your sink will still
receive every item at least once. If you ensure that after the `flushFn`
is called all the previous items are persistently stored, your sink
provides an at-least-once guarantee. If you don't (like our first
example without the flushFn), your sink can also miss items. If the
system you’re storing the data into is idempotent (i.e. writing the same
thing multiple times has the exact same effect as writing it a single
time - obviously not the case with our example), then your sink will
have an exactly-once guarantee.

## Source

Let's write a source which is capable of reading the data that has been
output by our [example sink](#sink). We will start with a simple, batch
version:

```java
BatchSource<String> cpuSource = SourceBuilder
    .batch("cpu-source", x -> new BufferedReader(new FileReader("data.csv")))
    .<String>fillBufferFn((in, buf) -> {
        String line = in.readLine();
        if (line != null) {
            buf.add(line);
        } else {
            buf.close();
        }
    })
    .destroyFn(buf -> buf.close())
    .build();
```

We basically specified:

* how to set up the resources we need (the `createFn`), which is
  basically just a `BufferedReader`
* how to retrieve data to be emitted (the `fillBufferFn`), which will be
  called by Jet whenever it needs more data
* how to tear down the resources we have used once we are done (the
  `destroyFn`)

We can run it in a dummy pipeline, to see that it works and that it
really retrieves data from the file:

```java
Pipeline p = Pipeline.create();
p.readFrom(cpuSource)
 .writeTo(Sinks.logger());

JetInstance jet = Jet.bootstrappedInstance();
jet.newJob(p).join();
```

### Batching

Our source works, but it's not efficient, because it always just
retrieves one line at a time. Optimally the `fillBufferFn` should fill
the buffer with all the items it can acquire without blocking. As a
rule of thumb 100 items at a time is enough to dwarf any per-call
overheads within Jet.

The function may block as well, if need be, but taking longer than a
second to complete can have negative effects on the overall performance
of the processing pipeline.

To make it more efficient we could change our `fillBufferFn` like this:

```java
(in, buf) -> {
    for (int i = 0; i < 128; i++) {
        String line = in.readLine();
        if (line == null) {
            buf.close();
            return;
        }
        buf.add(line);
    }
}
```

### Unbounded

Custom sources don't need to batching ones. They can also provide
*unbounded* data. Let's see an example of such a `StreamSource`, one
that reads lines from the network:

```java
StreamSource<String> socketSource = SourceBuilder
    .stream("http-source", ctx -> {
        int port = 11000;
        ServerSocket serverSocket = new ServerSocket(port);
        ctx.logger().info(String.format("Waiting for connection on port %d ...", port));
        Socket socket = serverSocket.accept();
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(socket.getInputStream()));
        ctx.logger().info(String.format("Data source connected on port %d.", port));
        return reader;
    })
    .<String>fillBufferFn((reader, buf) -> {
        for (int i = 0; i < 128; i++) {
            if (!reader.ready()) {
                return;
            }
            String line = reader.readLine();
            if (line == null) {
                buf.close();
                return;
            }
            buf.add(line);
        }
    })
    .destroyFn(reader -> reader.close())
    .build();
```

### Timestamps

We could make use of our stream source via the following pipeline:

```java
Pipeline p = Pipeline.create();
p.readFrom(socketSource)
 .withoutTimestamps()
 .writeTo(Sinks.logger());

JetInstance jet = Jet.bootstrappedInstance();
jet.newJob(p);
```

One thing to note here is that there is an extra line
(`withoutTimestamps`) which is needed because for stream sources Jet
has to know what kind of event timestamps they will provide (if any). Now
we are using it without timestamps, but this unfortunately means that
we aren't allowed to use [Windowed Aggregation](windowing.md) in
our pipeline.

There are multiple ways to fix this (we can add timestamps in the
pipeline after the source), but the most convenient one is to provide
the timestamps right in the source.

Let's assume the data that will come in over the network is in the same
format as the one output by our [example sink](#sink). In that case we
know that each line starts with a timestamp, so we could modify our
source like this:

```java
StreamSource<String> socketSource = SourceBuilder
    .timestampedStream("http-source", ctx -> {
        int port = 11000;
        ServerSocket serverSocket = new ServerSocket(port);
        ctx.logger().info(String.format("Waiting for connection on port %d ...", port));
        Socket socket = serverSocket.accept();
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        ctx.logger().info(String.format("Data source connected on port %d.", port));
        return reader;
    })
    .<String>fillBufferFn((reader, buf) -> {
        for (int i = 0; i < 128; i++) {
            if (!reader.ready()) {
                return;
            }

            String line = reader.readLine();
            if (line == null) {
                buf.close();
                return;
            }

            buf.add(line, Long.parseLong(line.substring(0, line.indexOf(','))));
        }
    })
    .destroyFn(reader -> reader.close())
    .build();
```

The pipeline changes too:

```java
Pipeline p = Pipeline.create();
p.readFrom(socketSource)
 .withNativeTimestamps(0)
 .writeTo(Sinks.logger());
```

### Parallelism

In the examples we showed so far the source was non-distributed: Jet
will create just a single processor in the whole cluster to serve all
the data. This is an easy and obvious way to create a source connector.

If you want to create a distributed source, the challenge is
coordinating all the parallel instances to appear as a single, unified
source.

In our somewhat contrived example we could simply make each instance
to listen on its own separate port. We can achieve this by modifying
the `createFn` and making use of the unique, global processor index
available in the `Processor.Context` object we get handed there:

```java
StreamSource<String> socketSource = SourceBuilder
    .timestampedStream("http-source", ctx -> {
        int port = 11000 + ctx.globalProcessorIndex();
        // ...
    })
    .<String>fillBufferFn(...)
    .destroyFn(...)
    .distributed(2)
    .build();
```

Notice that we have added an extra call to specify the local parallelism
of the source (the `distributed()` method). This means that each Jet
cluster member will now create two such sources.

If we now start a cluster with two members, we will be listening to four
different sockets:

```java
Pipeline p = Pipeline.create();
p.readFrom(socketSource)
 .withNativeTimestamps(0)
 .writeTo(Sinks.logger());

JetInstance jet = Jet.newJetInstance();
Jet.newJetInstance();
jet.newJob(p).join();
```

In the output you should see:

```text
Waiting for connection on port 11001 ...
Waiting for connection on port 11000 ...
Waiting for connection on port 11002 ...
Waiting for connection on port 11003 ...
```

### Fault Tolerance

If you want your source to behave correctly within a streaming Jet job
that has a processing guarantee configured (**at-least-once** or
**exactly-once**), you must help Jet with saving the operational state
of your context object to the snapshot storage.

There are two functions you must supply:

* **`createSnapshotFn`** returns a serializable object that has all the data
  you’ll need to restore the operational state
* **`restoreSnapshotFn`** applies the previously saved snapshot to the
  current context object

While a job is running, Jet calls `createSnapshotFn` at regular
intervals to save the current state.

When Jet resumes a job, it will:

* create your context object the usual way, by calling `createFn`
* retrieve the latest snapshot object from its storage
* pass the context and snapshot objects to `restoreSnapshotFn`
* start calling `fillBufferFn`, which must start by emitting the same
  item it was about to emit when createSnapshotFn was called.

You’ll find that `restoreSnapshotFn`, somewhat unexpectedly, accepts not
one but a list of snapshot objects. If you’re building a simple,
non-distributed source, this list will have just one element. However,
the same logic must work for distributed sources as well, and a
distributed source runs on many parallel processors at the same time.
Each of them will produce its own snapshot object. After a restart the
number of parallel processors may be different than before (because you
added a Jet cluster member, for example), so there’s no one-to-one
mapping between the processors before and after the restart. This is why
Jet passes all the snapshot objects to all the processors, and your
logic must work out which part of their data to use.

Here’s a brief example with a fault-tolerant streaming source that
generates a sequence of integers:

```java
StreamSource<Integer> faultTolerantSource = SourceBuilder
    .stream("fault-tolerant-source", processorContext -> new int[1])
    .<Integer>fillBufferFn((numToEmit, buffer) ->
        buffer.add(numToEmit[0]++))
    .createSnapshotFn(numToEmit -> numToEmit[0])
    .restoreSnapshotFn(
        (numToEmit, saved) -> numToEmit[0] = saved.get(0))
    .build();
```

The snapshotting function returns the current number to emit, the
restoring function sets the number from the snapshot to the current
state. This source is non-distributed, so we can safely do
`saved.get(0)`.
