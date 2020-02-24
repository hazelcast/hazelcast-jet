---
title: Streaming and Event Time
id: event-time
---

In an unbounded stream of events, the dimension of time is always there.
To appreciate this, consider a bounded stream: it may represent a
dataset labeled "Wednesday", but the computation itself doesn't have
to know this. Its results will be understood from the outside to be
"about Wednesday".

An endless stream, on the other hand, delivers information about the
reality as it is unfolding, in near-real time, and the computation
itself must deal with time explicitly.

## But which time(stamp) to use

When dealing with the events used in a stream processing system, there
are typically at least two domains of time to consider:

* **Processing time**: this is the time at which events are observed in
  the system.
* **Event time**: this is the time at which events actually occurred.

In an ideal world event time and processing time would always be the
same, with events being processed immediately as they occur. In reality
however this is not the case, there can be a significant difference
between the two. The difference is also highly variable and is affected
by factors like: network congestion, shared resource limitations and
many more.

Depending on the use case, streaming systems usually rely on event time
in order to provide more meaningful results, although there are
situations when processing time will also do or is the only option
available.

## Using time

With unbounded streams you need a policy that selects bounded chunks
whose aggregate results you are interested in. This is called
*windowing*. You can imagine the window as a time interval laid over the
time axis. A given window contains only the events that belong to that
interval. Probably the most natural kind of window is the *sliding
window*: it slides along the time axis, trailing just behind the current
time.

Sliding window aggregation is a great tool to discover the dynamic
properties of your event stream. Quick example: say your event stream
contains GPS location reports from millions of mobile users. With a few
lines of code you can split the stream into groups by user ID and apply
a sliding window with linear regression to retrieve a smoothened
velocity vector of each user. Applying the same kind of window the
second time will give you acceleration vectors, and so on.

## Event Time Disorder

When using event time instead of processing time there is always the
possibility of observing the events out of their true order of
occurrence.

Processing events out of order is a challenge. Hazelcast Jet handles
most of the concerns internally, but there is one decision it can't make
for you: how much to wait for lagging events. Jet can't emit the result
of a windowed aggregation until it has received all the events belonging
to the window, but the longer it waits, the later you'll see the results.
So you must strike a balance and choose how much to wait. This parameter
is called the *allowed event lag*.
