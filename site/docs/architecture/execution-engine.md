---
title: Cooperative Threading
---

Job execution is done on a user-configurable number of threads. Each
worker thread has a list of processors of which it is in charge. As the
processors complete at different rates, the unutilized processors back
off and stay idle, allowing busy workers to catch up to keep the load
balanced.

![Hazelcast Jet Parallelism Model](assets/cooperative-multithreading.png)

This approach, called cooperative multi-threading, is one of the core
features of Hazelcast Jet and can be roughly compared to green threads.
Each processor can perform a small amount of work each time it is
invoked, then yield back to the Hazelcast Jet engine. The engine manages
a thread pool of fixed size, and the instances take their turn on each
thread in a round-robin fashion.

The purpose of cooperative multi-threading is to improve performance.
 Two key factors contribute to this:

- The overhead of context switching is much lower since the operating
 system’s thread scheduler is not involved.
- The worker thread driving the instances stays on the same core for
 longer periods, preserving the CPU cache lines.