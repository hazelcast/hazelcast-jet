---
title: Directed Acylic Graph (DAG)
id: dag
---

Hazelcast Jet models computation as a network of tasks connected with
data pipes. The pipes are one-way: results of one task are the input of
the next task. Since the dataflow must not go in circles, the structure
of the network corresponds to the notion of a Directed Acyclic Graph
&ndash; DAG.

## The Word Count DAG

Let's take one specific problem, the Word Count, and explain how to
model it as a DAG. In this task we analyze the input consisting of lines
of text and derive a histogram of word frequencies. Let's start from the
single-threaded Java code that solves the problem for a basic data
structure such as an `ArrayList`:

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
apply our lambda functions as plug-ins:

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

The tasks are connected into a cascade, forming the following DAG:

![Word Count DAG](assets/dag.png)

With the computation in this shape, Jet can now easily parallelize it by
starting more than one parallel task for a given step. It can also
introduce a network connection between tasks, sending the data from one
cluster node to the other. This is the basic principle behind Jet's
auto-parallelization and distribution.
