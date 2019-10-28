# Hazelcast Jet 

Hazelcast Jet is an open-source, cloud-native, distributed stream and batch processing engine.

## What's included

* *bin/jet-start.sh/.bat*: Command for starting a new Jet instance
* *bin/jet-stop.sh/.bat*: Command for stopping all running Jet instances
* *bin/jet.sh/.bat*: The Jet Command Line Client, used to submit jobs and interact with the cluster (i.e. list running jobs, cancel a job)
* *bin/jet-cluster-admin.sh*: Command line for make cluster-wide state changes (i.e. shutdown whole cluster)
* *config/hazelcast-jet.yaml*: The jet configuration file
* *config/hazelcast.yaml*: The IMDG configuration used by the Jet instance
* *config/hazelcast-client.yaml*: The client configuration file used by the Jet Command Line client
* *config/log4j.properties*: Logging configuration used by the Jet Instance
* lib: Optional connectors for Jet
    * Kafka
    * Hadoop
    * S3
    * Avro

## Quickstart

### 1. Starting a Jet Instance

On a terminal prompt, enter the command below:

```
$ bin/jet-start.sh
```

### 2. (optional) Start a second node to form a cluster

Repeat the first step on the terminal and you should see output like below, after the nodes have 
discovered each other. 

```
Members {size:2, ver:2} [
	Member [192.168.0.2]:5701 - 399aa674-cc73-4bae-8451-67943efc4a66 this
	Member [192.168.0.2]:5702 - 79b6399b-433f-4d61-b0d9-38ec121af07b
]
```

Note: By default multicast is used, which may be disabled in some environments. In this case, please
have a look at enabling the TCP-IP join inside `config/hazelcast.yaml`. For more details, please
see the [section on Hazelcast manual](https://docs.hazelcast.org/docs/3.12.3/manual/html-single/index.html#setting-up-clusters)

### 3. Submitting a Job

```
$ bin/jet.sh submit examples/hello-word.jar
```


### Additional Information

* [Hazelcast Jet on GitHub](https://github.com/hazelcast-jet)
* [Hazelcast Jet Homepage](https://jet.hazelcast.org)
