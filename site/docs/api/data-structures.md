---
title: Distributed Data Structures
description: Distributed data structures inherited from Hazelcast and how Jet makes use of them.
---

Jet is built on top of
[Hazelcast](https://github.com/hazelcast/hazelcast) which is an open
source in-memory data grid. Hazelcast provides a wide variety of
distributed data structures and concurrency primitives, including:

* A distributed, partitioned and queryable in-memory key-value store
  implementation, called `IMap`
* Additional data structures and simple messaging constructs such as
  `Set`, `MultiMap`, `Queue`, `Topic`
* A cluster-wide unique ID generator, called `FlakeIdGenerator`
* A distributed,
  [CRDT](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type)
  based counter, called `PNCounter`
* A cardinality estimator based on [`HyperLogLog`](https://en.wikipedia.org/wiki/HyperLogLog)

Additionally, Hazelcast includes a full [Raft](https://en.wikipedia.org/wiki/Raft_(computer_science))
implementation which allows implementation of _linearizable_ constructs
such as:

* A distributed and reentrant lock implementation, called `FencedLock`
* Distributed implementations of `AtomicLong`, `AtomicReference` `CountDownLatch`

Hazelcast data-structures are in-memory, highly optimized and offer very
low latencies. For a single `get` or `put` operation on a `IMap`, you
can typically expect a round-trip-time of under _100 microseconds_.

As Jet includes a full implementation of Hazelcast, all of these
features are also available to Jet users. To access the underlying
Hazelcast features you can use the following syntax:

```java
JetInstance jet = Jet.bootstrappedInstance();
PNCounter counter = jet.getHazelcastInstance().getPNCounter("counter");
long prev = counter.getAndIncrement(1);
```

Some of these data structures are also exposed through `JetInstance`,
for example:

```java
JetInstance jet = Jet.bootstrappedInstance();
IMap<String, User> userCache = jet.getMap("users");
```

Jet is integrated with these data structures and also internally makes
use of them as part of the job execution engine. For example, to store
job state and metadata, among other things.

## IMap

`IMap` is the main data structure in Hazelcast and is an implementation
of the java `ConcurrentMap` interface, with support for additional
operations. It's distributed and partitioned across the whole cluster.
`IMap` supports many different operations, but the two canonical
ones are `put` and `get`:

```java
IMap<String, User> userCache = jet.getMap("users");
userCache.put(user.getId(), user);
```

Then, at a later point in time, even from a different application, you
may retrieve the same object by:

```java
IMap<String, User> userCache = jet.getMap("users");
User user = userCache.get("user-id-to-retrieve");
```

### Partitioning

The default partition count in Hazelcast is `271`. This means that there
are 271 buckets (_partitions_) for keys and each key is assigned to a
partition, based on the _hash code_ of its binary representation
(_serialization_).

The partition for each key is deterministic, for example, if you
repeatedly put the same key into a map, it would always end up in the
same partition.

Each partition has one or more backups in other nodes so that if one of
the nodes were to go down, the backup partition would take over and
become the primary. This ensures that no data is lost in face of node
failures. By default, each partition is configured to have one backup.

### Replication

When a new entry is added to/removed from or updated in the map, the
change is replicated to the other nodes. By default, the backups are
_synchronous_ which means that the caller (i.e. the client) will not
receive a response until the operation has been replicated to the
backups.

Having more backups than the default of `1` means that you can tolerate
more than `1` node failing at a time, however they incur a memory cost:
each backup occupies the same amount of space as a primary partition,
meaning that over the whole cluster you will be consuming _n_ times the
memory, where _n_ is the number of backups.

Hazelcast also supports _asynchronous_ backups, for which there will be
no wait when returning a response. This means that the caller may not be
sure that the data has been replicated.

### Scaling

When a new members joins the cluster, some of the partitions will
automatically be migrated to it. In this way Hazelcast is able to fully
utilize the memory of the new node. Likewise, when a node leaves the
cluster, the remaining nodes will become the primaries and backups,
increasing the memory usage on those nodes.

### EntryProcessor

A `ConcurrentMap` supports atomic updates through the use of methods
such as `ConcurrentMap.compute`. An `EntryProcessor` is the equivalent
feature in `IMap`, where you can make atomic updates to the map without
having to retrieve the entry, update the value and put it back. For example,
imagine the following code:

```java
IMap<String, User> userCache = jet.getMap("users");
User user = userCache.get("user-id");
user.setAccessCount(user.getAccessCount() + 1);
userCache.put("user-id", user);
```

While this code looks innocent, it has two problems:

1. You have to do two round-trips to retrieve the entry and then to
   update it.
2. It is not atomic. What if some other client also updated the same
   entry at the same time? One of the increments may be lost.
   This is typically known as a _race condition_.

To avoid these problems, you can use an `EntryProcessor` instead:

```java
static class IncrementEntryProcessor implements EntryProcessor<String, User, User> {
    @Override
    public User process(Entry<String, User> entry) {
        User value = entry.getValue();
        value.setAccessCount(value.getAccessCount() + 1);
        return entry.setValue(value);
    }
}

IMap<String, User> userCache = jet.getMap("users");
userCache.executeOnEntry("user-id", new IncrementEntryProcessor());
```

>Note that the `EntryProcessor` must be on the classpath of the node or
>loaded in some other way. See the [classloading](classloading) section
>for more details.

### Querying

Hazelcast Maps also support indexing and querying, with the use of
`Predicate`s.

Querying is a powerful feature which allows you to treat the map almost
like a database:

```java
IMap<String, User> userCache = jet.getMap("users");
userCache.addIndex(IndexType.SORTED, "age");
List<User> user = userCache.values(Predicates.greaterThan(35));
```

### Additional Features

`IMap` provides many additional features such as:

* Eviction - expiring items automatically after a _time-to-live_ (TTL)
  period, or based on other heuristics (such as number of items or total
  memory usage)
* `MapStore` and `MapLoader`, for implementing _read-through_,
  _write-through_ and _write-behind_ caching patterns.
* `MapListener` for observing updates to the map from a client
  application
* JSON support through `HazelcastJsonValue` which allows fast indexing
  and querying of JSON values.
* `NearCache` allows clients to provide an additional, smaller layer of
  2nd level caching thus even avoiding the cost of a network round-trip.

For more information about the advanced features of `IMap`, the best
source of information is Javadoc and the [Hazelcast Reference
Manual](https://hazelcast.org/documentation/).

### Consistency Guarantees

`IMap` is heavily optimized for high-throughput and low-latency and is an
[AP](https://en.wikipedia.org/wiki/CAP_theorem) data structure, and
generally offers strong consistency guarantees (the writes will be seen
by other processes in same order), but this is not always guaranteed and
is done in a best-effort basis. For an in-depth discussion regarding
this please see the [In-Memory
Architecture](../architecture/in-memory-storage)

## ReplicatedMap

`ReplicatedMap` has a similar API to `IMap`, with some important
distinctions: Instead of being partitioned, values are replicated to all
the nodes. As a result, each entry will consume more memory in the
cluster, but each member will have access to the whole entry set.

The API usage is same as IMap:

```java
ReplicatedMap<String, User> userCache = jet.getReplicatedMap("users");
User user = userCache.get("user-id");
```

When used within Jet within the context of a [`mapUsingReplicatedMap`](../api/stateless-transforms#mapusingreplicatedmap)
transform this enables faster lookups, as the lookup is always data-local.

Unlike `IMap`, `ReplicatedMap` offers weak consistency guarantees as updates
are always done locally and then replicated, so that writes may be
observed by different processes in different order.

## IList, ISet and IQueue

`IList`, `ISet` and `IQueue` are relatively simple data structures that
offer the Java equivalent APIs such as `List`, `Set` and `Queue`, but
are not partitioned, so they're constrained to a single partition.

They're only recommended to be used for smaller data sets or simple
coordination tasks.
