---
title: Event Time and Stream Processing
id: event-time
---

## The Importance of "`Right Now`"

In batch jobs the data we process represents a point-in-time snapshot of
our state of knowledge (for example, warehouse inventory where
individual data items represent items on stock). We can recapitulate
each business day by setting up regular snapshots and batch jobs.
However, there is more value hiding in the freshest data &mdash; our
business can win by reacting to minute-old or even second-old updates.
To get there we must make a shift from the finite to the infinite: from
the snapshot to a continuous influx of events that update our state of
knowledge. For example, an event could pop up in our stream every time
an item is checked in or out of the warehouse.

A single word that captures the above story is *latency*: we want our
system to minimize the latency from observing an event to acting upon
it.

## Windowing

In an unbounded stream, the dimension of time is always there.  Consider
a batch job: it may process a dataset labeled "`Wednesday`", but the
computation itself doesn't have to know this. Its results will be
understood from the outside to be "`about Wednesday`". An endless stream,
on the other hand, delivers information about the reality as it is
unfolding, in near-real time, and the computation itself must deal with
time explicitly.

Another point: in a batch it is obvious when to stop aggregating and
emit the results: when we have exhausted the whole dataset. However,
with unbounded streams we need a policy on how to select bounded chunks
whose aggregate results we are interested in. This is called
*windowing*. We imagine the window as a time interval laid over the time
axis. A given window contains only the events that belong to that
interval.

A very basic type of window is the *tumbling window*, which can be
imagined to advance by tumbling over each time. There is no overlap
between the successive positions of the window. In other words, it
splits the time-series data into batches delimited by points on the time
axis. The result of this is very similar to running a sequence of batch
jobs, one per time interval.

A more useful and powerful policy is the *sliding window*: instead of
splitting the data at fixed boundaries, it lets it roll in
incrementally, new data gradually displacing the old. The window
(pseudo)continuously slides along the time axis.

Another popular policy is called the *session window* and it's used to
detect bursts of activity by correlating events bunched together on the
time axis. In an analogy to a user's session with a web application,
the session window "`closes`" when the specified session timeout elapses
with no further events.