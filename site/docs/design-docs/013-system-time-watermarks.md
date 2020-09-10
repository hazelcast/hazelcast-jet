---
title: 013 - Resolving Sparse Events Issue with System-Time Watermarks
description: Make the time progress independently from the event rate
---

*Target Release*: 4.3

## The _Sparse Events Issue_

When using event time, the time progresses only through the ingestion of
new events. If the events are sparse, time will effectively stop until a
newer event arrives. This causes high latency for time-sensitive
operations (such as window aggregation). In addition, Jet tracks event
time for every source partition separately, and if just one partition
has sparse events, time progress in the whole job is hindered.

## Ingestion Time

Instead of a timestamp extracted from the event, a timestamp will be
assigned to each event using the cluster member's system clock. This way
of assigning timestamps is inferior because the event isn't associated
with a time at which it occurred, but with a time when it entered the
processing, and if the job restarts due to a crash, reprocessed events
will be assigned a different timestamp after the restart. On the other
hand, it provides the best latency: the job will never wait for delayed
items, there's no need for allowed time lag as with event-time
processing.

The ingestion time is enabled by calling `withIngestionTime()` in the
Pipeline API.

## The Improvement

First we've implemented `WatermarkPolicy.limitingRealTimeLag()`. This
policy emits a watermark that lags behind the system clock by a
specified lag. This policy doesn't depend on the events processed, it
depends solely on the system clock. Currently, this policy is available
only in Core API.

Secondly, we've made `StreamSourceStage.withIngestionTimestamps()` to
use this policy with zero lag. We can do this because the events are
assigned with system time and we're sure that there will never be an
event with an older timestamp.

Further work is to allow the use of real-time watermarks in Pipeline API
even for event-time processing: this will solve the sparse events issue
and will give fixed latency relative to the real time, but in case the
job is down for more than the real time lag, some events will be
dropped as late.
