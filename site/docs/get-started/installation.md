---
title: Installing a standalone cluster
id: installation
---

In this section we'll look at various way to install a standalone Hazelcast
Jet cluster and submit the job that we created earlier.

## Install as a Standalone Server

If you are setting up Jet as a standalone cluster, download the
[distribution package](https://jet.hazelcast.org/download) of Hazelcast Jet
and use the `bin/start-jet` command to start an instance.

The distribution package contains a ready-made JAR,
`examples/hello-world.jar`, that you can use to run a simple smoke
test. In the base directory of the Jet distribution, type:

```bash
$ bin/jet-start -d
... wait for Jet to start ...
$ bin/jet submit examples/hello-world.jar
```

If everything works out, you should see this in the log output:

```
INFO [HelloWorld] [main] - Top 10 random numbers observed so far in the stream are:
```

This uses a simple Jet cluster of size 1, but you can use the same file
to verify your full cluster setup as well.

## Install using Docker

