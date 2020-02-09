---
title: Installing a standalone cluster
id: installation
---

Now that you have submitted a job to an embedded node, the next step is
to setup an actual Jet cluster. After setting up the cluster, we'll 
submit the pipeline we previously created for it.

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

The distribution package also contains a pre-packaged Jet job in a JAR
that you can use to run to verify the installation test. After opening a
new terminal window, you can submit this job to the cluster using the
following command:

```bash
bin/jet submit examples/hello-world.jar
```

After the job is submitted you should see this in the log output:

```txt
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

TODO
