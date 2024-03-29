---
title: Installation
description: Jet Installation Guide
id: version-4.5.4-installation
original_id: installation
---

## Requirements

Jet requires a minimum of JDK 8 for installation. You can download
various pre-build JDK versions for different operation systems
including Linux, Windows and MacOS from [AdoptOpenJDK](https://adoptopenjdk.net/).

Jet itself is a small binary (slightly larger than 10MB) but as it is a
a highly-parallel distributed computational framework it is recommended
that it is run on a machine with several processors (or cores). We
recommend the following minimum as a good starting point:

* 8 Cores
* 16 GB RAM

Jet is tested to run on Linux machines, but it's compatible with any
operating system where the JDK is available.

## Jet Distributions

There are multiple distribution packages available to
[download](/download) - regular and slim.

The regular distribution contains Jet core engine, extension modules,
management scripts, example configuration files. If you are looking
for easy to use, includes-all kind of distribution, this should be your
choice.

The slim distribution doesn't contain any extension modules and is
suitable for deployments, where size is a concern, e.g., a Docker
image. The extension modules can be downloaded separately from the
[download](/download) page.

## Jet Home Folder

Once you have [downloaded](/download) Jet, the installation has the
following structure:

* `bin`: executable scripts
* `config`: configuration files used by Jet
* `examples`: Folder containing sample jobs which can be submitted to
  the cluster
* `logs`: Folder for Jet process' log files
* `lib`: JAR files for Jet and the extension modules. Everything in this
  folder is automatically added to classpath during node startup.

The `bin` folder has the following scripts:

* `bin/jet-start`: starts a new Jet instance
* `bin/jet-stop`: stops all Jet instances running on this machine
* `bin/jet`: tool for submitting and managing jobs
* `bin/jet-cluster-admin`: tool for managing the Jet cluster (for
  example, for gracefully shutting down the cluster)
* `bin/jet-cluster-cp-admin`: tool for managing the
  [CP Subsystem](../api/data-structures#cp-subsystem) in the Jet cluster

The following files are present in the `config` folder:

* `config/hazelcast-jet.yaml`: The Jet configuration file
* `config/hazelcast.yaml`: The Hazelcast configuration used by the Jet
  node
* `config/hazelcast-client.yaml`: The client configuration file used by
  the Jet Command Line client
* `config/log4j.properties`: Logging configuration used by the Jet
  Instance
* `config/examples`: Extended config files which show all the possible
  config options

## Starting Jet in Daemon Mode

You can start the jet node in daemon mode using the following command:

```bash
bin/jet-start -d
```

In this mode, the standard out and error will be written to the `logs`
folder.

## Configuring JVM parameters

You can configure the required JVM parameters such as heap size in the
`jvm.options` file in the `config/` folder. Use one option per line. You
can also use `#` at the beginning of the line for comments.

For example, to start Jet with a 8GB heap use the following content in
the `jvm.options` file:

```text
# JVM Configuration
-Xmx8G
```

You can also use the `JAVA_OPTS` environment variable for ad-hoc
configuration option. For example, to start Jet with a 8GB heap you can
use the following command:

```bash
JAVA_OPTS=-Xmx8G bin/jet-start
```

You can use the `JAVA_OPTS` also to pass additional properties to Jet:

```bash
JAVA_OPTS=-Dhazelcast.operation.thread.count=4 bin/jet-start
```

Refer to the [Configuration](configuration) section for a list of
properties.

## Configuring Classpath

You can add additional JARs to classpath by using the `CLASSPATH`
environment variable if you don't want to add them to the `lib` folder.
For example:

```bash
CLASSPATH=path/to/lib1.jar:path/to/lib2:path/lib/* bin/jet-start
```

This will add the specified JARs to the classpath.

## Specifying which JDK to use

Jet by default will use the JDK configured by `JAVA_HOME` environment
variable. If this variable is not specified it will try to use the
`java` command available in the OS. The JDK and parameters used will
be output during note startup as below:

```text
########################################
# JAVA=/usr/bin/java
# JAVA_OPTS=--add-modules java.se --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
..
# CLASSPATH=/home/jet/hazelcast-jet-4.5.4/lib/*:
########################################
```
