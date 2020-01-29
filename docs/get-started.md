---
id: get-started
title: Getting Started
sidebar_label: Getting Started
---

## Get Started

In this section we'll get you started using Hazelcast Jet. We'll show
you how to set up a Java project with the proper dependencies and a
quick Hello World example to verify your setup.

## Requirements

In the good tradition of Hazelcast products, Jet is distributed as a JAR
with no other dependencies. It requires JRE version 8 or higher to run.

### Use Hazelcast Jet as an Embedded Runtime

If you want to use Jet embedded into your existing distributed
application, all you need is a single Jet JAR on your classpath. These
are the Maven and Gradle snippets you can use:

```xml
<dependencies>
  <dependency>
    <groupId>com.hazelcast.jet</groupId>
    <artifactId>hazelcast-jet</artifactId>
    <version>{jet-version}</version>
  </dependency>
</dependencies>
```

```
compile 'com.hazelcast.jet:hazelcast-jet:{jet-version}'
```

### Install Jet as a Standalone Cluster

If you are setting up Jet as a standalone cluster, download the
[distribution package](https://jet.hazelcast.org/download) of Hazelcast Jet
and use the `bin/start-jet` command to start an instance.

The distribution package contains a ready-made JAR,
`examples/hello-world.jar`, that you can use to run a simple smoke
test. In the base directory of the Jet distribution, type:


```bash
$ bin/jet-start &
... wait for Jet to start ...
$ bin/jet submit examples/hello-world.jar
```

If everything works out, you should see this in the log output:

```
INFO [HelloWorld] [main] - Top 10 random numbers observed so far in the stream are:
```

This uses a simple Jet cluster of size 1, but you can use the same file
to verify your full cluster setup as well.