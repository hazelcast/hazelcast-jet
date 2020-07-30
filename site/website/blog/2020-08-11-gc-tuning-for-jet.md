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
cores, the GC could have a slice of the CPU all for itself and wouldn't
interfere with Jet's latency. In most low-latency use cases, the
application doesn't need 100% CPU, but it needs its share of the CPU
100% of the time.

Our hopes materialized in the benchmark: this trick had a drammatic
impact on the latency with all three garbage collectors we tested
(Shenandoah, ZGC and G1). The most important outcome was that we were
now able to push G1 below the 10 ms line. Since G1 is stable across a
wide range of throughputs, we immediately got it to perform within 10 ms
at double the throughput than in the previous round.

# The Setup

Our setup is for the most part identical to the one in the previous
post. We refreshed the code a bit, we now use the released version 4.2
of Hazelcast Jet, as well as the released version of JDK 14.0.2 which
we used for Shenandoah. For G1 and ZGC we used Oracle OpenJDK 15 EA33,
which doesn't include Shenandoah.

We also implemented a parallelized event source simulator. Its higher
peak throughput allows it to catch up faster after a hiccup, helping
to reduce the latency a bit more.

We determined how many threads the given GC uses, set the size of the
Jet thread pool to 16 (c5.4xlarge vCPUs) minus that value and then did
some trial-and-error runs to find the optimum. Shenandoah takes 4
threads for itself, so we gave Jet 12. G1 uses 3 threads, so we gave Jet  
13\. Finally, ZGC uses just 2 threads, but we found Jet to perform a bit
better with 13 instead of the theoretical 14 threads, so we used that.
We also experimented with changing the GC's automatic choice for the
thread count, but didn't find a setting that would bet the default.

Additionally, with G1 we saw that in certain cases, even with
`MaxGCPauseMillis=5` (same as in the previous post), the size of the new
generation would grow large enough to negatively impact latency.
Therefore we added `MaxNewSize` with one of `100m`, `150m` and `200m`,
depending on the chosen throughput. This was also determined through
trial and error, the results seemed to be the best when a minor GC was
occurring about 10-20 times per second.

