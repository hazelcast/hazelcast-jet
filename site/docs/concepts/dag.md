---
title: Directed Acylic Graphs (DAG)
id: dag
---

Jet performs high performance in-memory data processing by modeling the
computation as a _Directed Acyclic Graph_ (DAG), where vertices
represent computation and edges represent data flows. A vertex receives
data from its inbound edges, performs a step in the computation, and
emits data to its outbound edges. Both the edge and the vertex are
distributed entities: there are many parallel instances of the
`Processor` type that perform a single vertex's computation work on
each cluster member. An edge between two vertices is implemented with
many data connections, both within a member (concurrent SPSC queues) and
between members (Hazelcast network connections).

One of the major reasons to divide the full computation task into
several vertices is _data partitioning_: the ability to split the data
stream into slices which can be processed in parallel, independently of
each other. This is how Jet can parallelize and distribute the
_group-and-aggregate_ stream transformation, the major workhorse in
distributed computing. To make this work, there must be a function which
computes the _partitioning key_ for each item and makes all related
items map to the same key. Jet can then route all such items to the same
processor instance, but has the freedom to route items with different
keys to different processors.

Typically your computation job consists of a _mapping_ vertex, where you
pre-process the input data into a form that's ready to be partitioned,
followed by the grouping-and-aggregating vertex. The edge between them
contains the partitioning logic.

## Modeling the Computation as a DAG

We'll take one specific problem, the Word Count, dissect it and explain
how it gets computed in a Jet cluster. The goal is to analyze the input
consisting of lines of text and derive a histogram of word frequencies.
Let's start from the single-threaded Java code that solves the problem
for a basic data structure such as an
`ArrayList`.:

```java
List<String> lines = someExistingList();
Map<String, Long> counts = new HashMap<>();
for (String line : lines) {
    for (String word : line.toLowerCase().split("\\W+")) {
        if (!word.isEmpty()) {
            counts.merge(word, 1L, (count, one) -> count + one);
        }
    }
}
```

Basically, this code does everything in one nested loop. This works for
the local `ArrayList`, very efficiently so, but in this form it is not
amenable to auto-parallelization. We need a _reactive_ coding style that
uses lambda functions to specify what to do with the data once it's
there while the framework takes care of passing it to the lambdas. This
leads us to the Pipeline API expression:

```java
Pipeline p = Pipeline.create();
p.readFrom(textSource())
 .flatMap(e -> traverseArray(e.getValue().toLowerCase().split("\\W+")))
 .filter(word -> !word.isEmpty())
 .groupingKey(wholeItem())
 .aggregate(AggregateOperations.counting())
 .writeTo(someSink());
```

In this form we can clearly identify individual steps taken by the
computation:

1. read lines from the text source
2. flat-map lines into words
3. filter out empty words
4. group by word, aggregate by counting
5. write to the sink

Now Jet can set up several concurrent tasks, each receiving the data
from the previous task and emitting the results to the next one, and
apply your lambda functions as plug-ins:

```java
// Source task
for (String line : textSource.readLines()) {
    emit(line);
}
```

```java
// Flat-mapping task
for (String line : receive()) {
    for (String word : flatMapFn.apply(line)) {
        emit(word);
    }
}
```

```java
// Filtering task
for (String word : receive()) {
    if (filterFn.test(word)) {
        emit(word);
    }
}
```

```java
// Aggregating task
Map<String, Long> counts = new HashMap<>();
for (String word : receive()) {
    String key = keyFn.apply(word);
    counts.merge(key, 1L, (count, one) -> count + one);
}
// finally, when done receiving:
for (Entry<String, Long> keyAndCount : counts.entrySet()) {
    emit(keyAndCount);
}
```

```java
// Sink task
for (Entry<String, Long> e: receive()) {
    sink.write(e);
}
```

With the computation in this shape, Jet can now easily parallelize it by
starting more than one parallel task for a given step. It can also
introduce a network connection between tasks, sending the data from one
cluster node to the other. This is the basic principle behind Jet's
auto-parallelization and distribution.
