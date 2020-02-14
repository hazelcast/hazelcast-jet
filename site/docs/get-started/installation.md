---
title: Install the Cluster
id: installation
---

Now that you've succeeded in running your first Jet program, it's time
to productionize it and run it on an actual cluster. Before involving
remote machines we can take the initial step of running a cluster on
your local machine. This will already be very close to the real thing
because all the communication happens over network interfaces and the
nodes just happen to reside on the same physical machine.

There are two main approaches to setting up a Hazelcast Jet cluster: as
a classic Java process and as a Docker instance. In both cases you'll
need the Hazelcast Jet distribution package and a local JDK installation
to interact with the cluster as a client. Download Jet from
[here](https://jet.hazelcast.org/download) and unzip it to a directory
we'll refer to as `<jet_home>`.

As for the JDK, you can get it from the [OpenJDK](https://openjdk.java.net/)
site and the minimum version is 8.


## Run as a Java Process

The distribution package contains everything you need to run Jet except
the JDK. From the `<jet_home>` directory issue this command:

```bash
$ bin/start-jet
```

This will start a Jet node in the foreground so you have to keep the
terminal window open. It is convenient because you can easily terminate
Jet with Ctrl-C or closing the window. In a production setting you can
use the `-d` option to start it in daemon mode.

The main entry point to interacting with the Jet cluster is the `jet`
command. For example, let's check the cluster state:

```bash
$ bin/jet cluster
State: ACTIVE
Version: 4.0-SNAPSHOT
Size: 1

ADDRESS                  UUID
[192.168.0.2]:5701       27a73154-f4bb-477a-aef2-27ffa6f03a2d
```

The distribution package also contains a pre-packaged Jet job in a JAR
that you can use to quickly verify the installation. You can submit it
like this:

```bash
$ bin/jet submit examples/hello-world.jar
```

You should see output like this:

```text
Top 10 random numbers in the latest window:
    1. 9,148,584,845,265,430,884
    2. 9,062,844,734,542,410,944
    3. 8,803,176,683,229,613,741
    4. 8,779,035,965,085,775,340
    5. 8,542,080,641,730,428,499
    6. 8,528,134,348,376,217,974
    7. 8,290,200,710,152,066,026
    8. 8,008,893,323,519,996,615
    9. 7,804,055,086,912,769,625
    10. 7,681,774,251,691,230,162
```

## Run as a Docker Instance

Hazelcast Jet maintains its official, self-contained Docker image. To
start a node, write this:

```bash
$ docker run hazelcast/hazelcast-jet
```

This should start a Hazelcast Jet node in a Docker container. Inspect
the log output for a line like this:

```text
Members {size:1, ver:1} [
	Member [172.17.0.2]:5701 - 4bc3691d-2575-452d-b9d9-335f177f6aff this
]
```

Note the IP address of the Docker container and use it in the commands
below instead of our example's `172.17.0.2`. Let's submit the hello world
application from the distribution package:

```bash
$ cd <jet_home>
$ docker run -it -v "$(pwd)"/examples:/examples hazelcast/hazelcast-jet jet -a 172.17.0.2 $ submit /examples/hello-world.jar
```

The command mounts the local `examples` directory from `<jet_home>` to
the container and uses Hazelcast Jet command-line tool to submit the
example JAR file to the cluster.

After the job is submitted you should see this in the log output:

```text
Top 10 random numbers in the latest window:
    1. 9,148,584,845,265,430,884
    2. 9,062,844,734,542,410,944
    3. 8,803,176,683,229,613,741
    4. 8,779,035,965,085,775,340
    5. 8,542,080,641,730,428,499
    6. 8,528,134,348,376,217,974
    7. 8,290,200,710,152,066,026
    8. 8,008,893,323,519,996,615
    9. 7,804,055,086,912,769,625
    10. 7,681,774,251,691,230,162
```
