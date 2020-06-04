---
title: Concerns Related to GC
description: Choosing the right JDK-GC combination is important.
---

## Choice of JDK and GC

In May 2020 we did extensive testing of Hazelcast Jet with several
combinations of JDK and garbage collector. We tried the following JDK/GC
combinations:

1. JDK 8 with the default Parallel collector and the optional
   ConcurrentMarkSweep and G1
2. JDK 11 with the default G1 collector and the optional Parallel
   collector
3. JDK 14 with the default G1 as well as the experimental Z and
  Shenandoah

These are our key findings:

1. As far as Hazelcast Jet is concerned, JDK version 8 is an antiquated
   runtime and shouldn't be used. None of its garbage collectors have
   satisfactory performance and their long GC pauses even result in
   nodes being kicked out of the cluster.
2. JDK 11 is the current Long-Term Support (LTS) version by Oracle and
   it is the lowest version of JDK we recommend. We found that its
   version of the G1, without any fine-tuning, already delivers
   next-generation GC performance. The default maximum GC pause target
   is 200 ms, which it meets in a very broad range of scenarios. By
   configuring a single JVM option (`-XX:MaxGCPauseMillis`), you can
   confine the maximum GC pause to 20-25 ms, which it can maintain at
   lower GC pressure levels.
4. There are two more GCs that are relevant, but as of this writing are
   in the experimental phase. They are the low-latency collectors:
   Oracle's Z and OpenJDK's Shenandoah. Testing on JDK 14, we found Z to
   be the better of the two and a viable replacement for G1 in scenarios
   that have low GC pressure, but very strict GC pause requirements. Z
   was able to keep the maximum pause within 10 milliseconds. Since the
   Z is available in OpenJDK as well, we recommend it for scenarios
   where G1's GC pauses are too long. The version of Z in JDK 14 has
   crude ergonomics to decide on a good background GC thread count, in
   some cases we were able to improve the throughput by using
   `-XX:ConcGCThreads`.

We tested the mentioned JDK/GC combinations in three different usage
scenarios, deriving some conclusions and recommendations we present
below.

### Low-Latency Unbounded Stream Processing, Moderate State

Example: detecting trends in 500 Hz sensor data from 100,000 devices and
sending corrective feedback within 10-20 milliseconds.

In this scenario the hardware must be provisioned to such a level that
the runtime doesn't even approach its maximum load, in order to stay
within the very strict latency limits. This is a use case where we
recommend the Z with the latest available JDK (version 14 as of this
writing).

### High-Throughput, Large-State Unbounded Stream Processing

Example: tracking GPS locations of millions of users, inferring their
velocity vectors.

In this scenario the state is large enough that just emitting the
results of aggregation may take several hundred milliseconds. This gives
us a ballpark for GC pause requirements as well. Hardware can be
provisioned so that the runtime is at high load, with both high heap
usage and high allocation rate. This kind of regime can push the GC to
its limits and beyond. The G1 collector on a modern JDK is the best
option here because the experimental low-latency GCs support much less
throughput. GC pauses will remain under 200 ms unless the GC is
overloaded, and the runtime will be able to sustain shorter bursts of
excessive throughput, gracefully increasing latency into the seconds
range. If the throughput demand allows it, you can reduce the GC pause
target below 100 ms.

### Batch Processing of Big Data Volumes

Example: analyzing a day's worth of stock trading data to update the
risk exposure of a given portfolio.

The relevant measure is time to complete, which implies a high
throughput demand, but since the data is being processed offline, there
is no additional latency requirement. The best option is the same as in
the previous scenario: a modern JDK with G1. The default maximum GC
pause of 200 ms is a good setting and allowing larger pauses may only
marginally help increase the throughput. We found that G1 can perform
well even at close to 90% heap usage.

### Garbage-Free Aggregation

The combination of keyed aggregation and batch processing means that
your state only grows throughout the computation. Once you see a key,
you maintain its aggregation state to the end. If the aggregation itself
doesn't release any objects once it has retained them, the whole
computation will be perfectly aligned with the Generational Garbage
Hypothesis: objects either die young or are retained until the end of
the computation. This reduces GC pressure and in our tests it boosted
the throughput of the batch pipeline by 30-35%.

For this reason we always strive to make the aggregate operations we
provide with Jet garbage-free. Examples are summing, averaging and
finding extremes. Our current implementation of linear tred, however,
does generate garbage because it uses immutable `BigDecimal`s in the
state.

If your requirements call for a complex aggregate operation not provided
by Jet, and if you use Jet for batch processing, putting extra effort
into implementing a custom garbage-free aggregate operation can be
worth it.
