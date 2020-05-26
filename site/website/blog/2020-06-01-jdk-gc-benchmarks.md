---
title: Benchmarking Jet with Different JDKs and GCs
description: Choosing the right JDK-GC combination is important.
---

The Java runtime has been evolving more rapidly in recent years and,
after two decades, we finally got a new default garbage collector: the
G1. Two more GCs are on their way to production and are available as
experimental features: Oracle's Z and OpenJDK's Shenandoah. We at
Hazelcast thought it was time to put all these new options to the test
and find which choices work well with distributed stream processing in
Hazelcast Jet.

Jet is being used for a broad spectrum of use cases, with different
latency and throughput requirements. Here are three important
categories:

1. Low-latency unbounded stream processing, with moderate state. Example:
  detecting trends in 500 Hz sensor data from 100,000 devices and
  sending corrective feedback within 10-20 milliseconds.
2. High-throughput, large-state unbounded stream processing. Example:
  tracking GPS locations of millions of users, inferring their velocity
  vectors.
3. Old-school batch processing of big data volumes. The relevant measure
  is time to complete, which implies a high throughput demand. Example:
  analyzing a day's worth of stock trading data to update the risk
  exposure of a given portfolio.

At the outset, we can observe the following:

- in scenario 1 the latency requirements enter the danger zone of GC
  pauses: 100 milliseconds, something traditionally considered an
  excellent result for a worst-case GC pause, may be a showstopper for
  many use cases
- scenarios 2 and 3 are similar in terms of demands on the garbage
  collector. Less strict latency, but large pressure on the tenured
  generation
- scenario 2 is tougher because latency, even if less so than in
  scenario 1, is still relevant

We tried the following combinations:

1. JDK 8 with the default Parallel collector and the optional G1
2. JDK 11 with the default G1 collector
3. JDK 14 with the default G1 as well as the experimental Z and
  Shenandoah

And here are our overall conclusions:

1. JDK 8 is an antiquated runtime. The Parallel collector enters huge
   Major GC pauses and the G1, although better than that, is stuck in an
   old version that uses just one thread when falling back to Full GC,
   again entering very long pauses. Even on a moderate heap of 12 GB,
   the pauses were exceeding 20 seconds.
2. On more modern JDK versions, the G1 is one monster of a collector. It
   handles heaps of dozens of GB with ease, keeping maximum GC pauses
   within 200 ms. Under extreme pressure it doesn't show brittleness
   with catastrophic failure modes. Instead the Full GC pauses rise into
   the low seconds range. Its Achilles' heel is the upper bound on the
   GC pause in favorable low-pressure conditions, about 20-25 ms.
3. The Z, while allowing substantially less throughput than G1, is very
   good in that one weak area of G1, offering worst-case pauses up to 10
   ms.
4. Shenandoah was a disappointment with occasional, but nevertheless
   regular, latency spikes up to 200 ms in the low-pressure regime.
5. Neither Z nor Shenandoah showed as smooth failure modes as G1.
  Shenandoah was slightly worse than Z, sometimes even throwing OOMEs
  where other GCs would just slow down.
