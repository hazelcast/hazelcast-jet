---
title: Sub-10 ms Latency in Java: Concurrent GC with Green Threads
description: Hazelcast Jet's green threads allow it to dedicate some CPU cores to GC and win big on latency
author: Marko Topolnik
authorURL: https://twitter.com/mtopolnik
authorImageURL: https://i.imgur.com/xuavzce.jpg
---

In a [previous post](/blog/2020/06/23/jdk-gc-benchmarks-rematch) we
showed that a modern JVM running live stream aggregation can achieve a
99.99% latency lower than 10 milliseconds. The focus of that post was
comparing the different GC options available for the JVM. In order to
maintain a level playing field, we kept to the default settings as much
possible.

In this round we wanted to look at the same problem from the opposite
angle: what can we do to help Hazelcast Jet achieve the best performance
available on a JVM? How much throughput can we get while staying within
the tight 10 ms bound for 99.99th percentile latency?

We found one major factor: since Jet uses [cooperative
multithreading](/docs/architecture/execution-engine) (similar to the
concepts of Green Threads and Coroutines), it always uses the same,
fixed-size thread pool no matter how many concurrent tasks it
instantiates to run a data pipeline. On the other hand, a concurrent
garbage collector creates its own thread pool to run the cleanup in the
background. So if we would shrink Jet's thread pool so that its size
plus the size of the GC thread pool doesn't exceed the number of CPU
cores, we may hope the GC would have a slice of the CPU all for itself
and wouldn't interfere with Jet's latency. In most low-latency use
cases, the application doesn't need 100% CPU, but it needs its share of
the CPU 100% of the time.

Our hopes materialized in the benchmark: this trick had a drammatic
impact on the latency with both garbage collectors we tested (G1 and
ZGC). The most important outcome was that we were now able to push G1
below the 10 ms line. Since G1 is stable across a wide range of
throughputs, we immediately got it to perform within 10 ms at double the
throughput than in the previous round.

## The Setup

Based on the expectations set by the previous benchmark, we focused on
the ZGC and G1 collectors and the latest pre-release of Java 15\. Our
setup stayed the same for the most part; we refreshed the code a bit and
now use the released version 4.2 of Hazelcast Jet with OpenJDK 15 EA33.

We also implemented a parallelized event source simulator. Its higher
throughput allows it to catch up faster after a hiccup, helping to
reduce the latency a bit more. The processing pipeline itself is
identical to the previous round,
[here](https://github.com/mtopolnik/jet-gc-benchmark/blob/round-3/src/main/java/org/example/StreamingRound3.java)
is the complete source code.

We determined how many threads the given GC uses, set the size of the
Jet thread pool to 16
([c5.4xlarge](https://aws.amazon.com/ec2/instance-types/c5/) vCPUs)
minus that value and then did some trial-and-error runs to find the
optimum. G1 uses 3 threads, so we gave Jet 13\. ZGC uses just 2 threads,
but we found Jet to perform a bit better with 13 instead of the
theoretical 14 threads, so we used that. We also experimented with
changing the GC's automatic choice for the thread count, but didn't find
a setting that would beat the default.

Additionally, with G1 we saw that in certain cases, even with
`MaxGCPauseMillis=5` (same as in the previous post), the size of the new
generation would grow large enough for Minor GC pauses to impact
latency. Therefore we added `MaxNewSize` with one of `100m`, `150m` and
`200m`, depending on the chosen throughput. This was also determined
through trial and error, the results seemed to be the best when a minor
GC was occurring about 10-20 times per second.

Summarizing, these are the changes we made with respect to the setup in
the previous post:

1. Reduced Jet's cooperative thread pool size
2. Parallel event source where previously it was single-threaded
3. Used the `MaxNewSize` JVM parameter for G1
4. Updated Hazelcast Jet and JDK versions

## The Results

Comparing ZGC's results below with those in the [previous
round](/blog/2020/06/23/jdk-gc-benchmarks-rematch#a-sneak-peek-into-upcoming-versions),
we can see the latency stayed about the same where it was already good,
but the range of throughputs got extended from 8 to 10 M items/second,
a solid 25% improvement.

The effect on G1 is sort of dual to the above: while the G1 already had
great throughput but fell just short of making it below the 10 ms line,
in this round its latency improved across the board, up to 40% at
places. The best news: **the maximum throughput at which a single
Hazelcast Jet node maintains 99.99% latency within 10 ms now lies at 20
million items per second**, a 250% boost!

![Latency on c5.4xlarge, 1 M Events per Second](assets/2020-08-05-latency-1m.png)

## Upgrading to 10 M Input Events per Second

Encouraged by this strong result, we dreamed up a scenario like this: we
have 100,000 sensors, each producing a 100 Hz measurement stream. Can a
single-node Hazelcast Jet handle this load and produce, say, the time
integral of the measured quantity from each sensor over a 1-second
window, at a 10 ms latency? This implies an order-of-magnitude leap in
the event rate, from 1 M to 10 M events per second, but also a reduction
in window length by the same factor, from ten seconds to one.

Nominally, the scenario results in the same combined input+output
throughput as well as about the same size of state that we already saw
work: 20 M items/second and 10 M stored map entries. It's the maximum
point where G1 was still inside 10 ms, but even at 25 M items/second it
still had pretty low latency. However, for reasons we haven't yet
identified, the input rate seems to have a stronger impact on GC, so
when we traded output for input, it turned out that G1 was nowhere near
handling it.

But, since we picked the c5.4xlarge instance type as a medium-level
option, for this "elite scenario" we considered the top-shelf EC2 box as
well: c5.metal. It commands 96 vCPUs and has some scary amount of RAM
that we won't need. On this hardware G1 decides to take 16 threads for
itself, so the natural choice would be 80 threads for Jet. However,
through trial and error we chased down the real optimum, which turned
out to be 64 threads. Here is what we got:

![Latency on c5.metal, 10 M Events per Second](assets/2020-08-05-latency-10m.png)

G1 comfortably makes it to the 20 M mark and then goes on all the way to
40 M items per second, gracefully degrading and reaching 60 M with just
12 ms. Beyond this point it was Jet who ran out of steam. The Jet
pipeline running at full speed just couldn't max out the G1! We repeated
the test with more threads given to Jet, 78, but that didn't make a
difference.
