---
title: Install the cluster
id: installation
---

Now that you have submitted a job to an embedded node, the next step is
to setup an actual Jet cluster. After setting up the cluster, we'll 
submit the pipeline we previously created to it.

There are two main recommended ways to set up a Hazelcast Jet cluster:

## Install as a Standalone Server

When setting up Jet as a standalone server, you need to download the
[distribution package](https://jet.hazelcast.org/download) of Hazelcast
Jet.

After downloading, unzip the distribution and use the following command
to start the node:

```bash
cd <jet_install_directory>
bin/start-jet
```

Note: Installing Jet this way requires at mininum JDK 8 runtime. You can
download the latest JDK from [OpenJDK](https://openjdk.java.net/).

```bash
bin/jet-start
```

This will start a Jet node in the foreground, you can use the `-d` option
to start it in daemon mode.

You can check the cluster state using the following command, which will
list the cluster status and all the active nodes:

```bash
$ bin/jet cluster
State: ACTIVE
Version: 4.0-SNAPSHOT
Size: 1

ADDRESS                  UUID               
[192.168.0.2]:5701       27a73154-f4bb-477a-aef2-27ffa6f03a2d
```

The distribution package also contains a pre-packaged Jet job in a JAR
that you can use to run to verify the installation test. After opening a
new terminal window, you can submit this job to the cluster using the
following command:

```bash
bin/jet submit examples/hello-world.jar
```

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

## Install using Docker

The official Docker images for Hazelcast Jet can be used to install 
Hazelcast Jet in Docker environment. 

Use the following command to start the node:

```bash
docker run hazelcast/hazelcast-jet
```

After seeing it started a new Hazelcast Jet node in a Docker container
with a log line similar to the below: 

```text
Members {size:1, ver:1} [
	Member [172.17.0.2]:5701 - 4bc3691d-2575-452d-b9d9-335f177f6aff this
]
```

Please note the IP address of the Docker container, we'll use it 
as a parameter to the command-line interface later on.

We will submit an example application which is included with the
[distribution package](https://jet.hazelcast.org/download) of Hazelcast
Jet.

After downloading, unzip the distribution and use the following commands
to submit the example job to the cluster running inside Docker. 

```bash
cd <jet_install_directory>
docker run -it -v "$(pwd)"/examples:/examples hazelcast/hazelcast-jet jet -a 172.17.0.2 submit /examples/hello-world.jar
```

The command basically mounts the local `examples` directory from the 
`<jet_install_directory>` to the container and uses Hazelcast Jet command-line 
tool to submit the example JAR file to the cluster. 

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
