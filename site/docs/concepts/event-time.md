---
title: Streaming and Event Time
id: event-time
---

In an unbounded stream of events, the dimension of time is always there.
To appreciate this, consider a bounded stream: it may represent a
dataset labeled "Wednesday", but the computation itself doesn't have
to know this. Its results will be understood from the outside to be
"about Wednesday". An endless stream, on the other hand, delivers
information about the reality as it is unfolding, in near-real time, and
the computation itself must deal with time explicitly.

With unbounded streams you need a policy that selects bounded chunks
whose aggregate results you are interested in. This is called
*windowing*. You can imagine the window as a time interval laid over the
time axis. A given window contains only the events that belong to that
interval. Probably the most natural kind is the *sliding window*: it
slides along the time axis, trailing behind the current time.

Sliding window aggregation is a great tool to discover the dynamic
properties of your event stream. Quick example: say your event stream
contains GPS location reports from millions of mobile users. With a few
lines of code you can split the stream into groups by user ID and apply
a sliding window with linear regression to retrieve a smoothened
velocity vector of each user. Applying the same kind of window the
second time will give you acceleration vectors, and so on.

## Event Time Disorder

If you want to treat time properly, every event must carry a timestamp.
You can't rely on the current time when processing an event because it
may be some arbitrarily later point in time. Also, you may easily end up
observing the events out of their true order of occurrence.

Processing events out of order is a challenge. Hazelcast Jet handles
most of the concerns internally, but there is one decision it can't make
for you: how much to wait for lagging events. Jet can't emit the result
of a windowed aggregation until it has received all the events belonging
to the window, but the longer it waits, the later you'll see the results.
So you must strike a balance and choose how much to wait. This parameter
is called the *allowed event lag*.
