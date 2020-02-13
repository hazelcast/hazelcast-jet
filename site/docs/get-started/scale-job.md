---
title: Scale your job
id: scale-job
---

Now that we have managed to submit our first job to the cluster successfuly,
let's make things a little more interesting.

One of the main features of Jet is that it allows you to dynamically
scale a job up or down. Jet keeps processing data without loss even when
a node fails, and you can add more nodes that immediately start sharing
the computation load.

First, let's make sure that the job that you submitted earlier in the previous section 
is still running:

<!--DOCUSAURUS_CODE_TABS-->
<!--Standalone-->
```bash
$ bin/jet list-jobs
ID                  STATUS             SUBMISSION TIME         NAME
03de-e38d-3480-0001 RUNNING            2020-02-09T16:30:26.843 N/A
```
<!--Docker-->
```bash
$ docker run -it hazelcast/hazelcast-jet jet -a 172.17.0.2 list-jobs
ID                  STATUS             SUBMISSION TIME         NAME
03e3-b8f6-5340-0001 RUNNING            2020-02-13T09:36:46.898 N/A
```
<!--END_DOCUSAURUS_CODE_TABS-->

## Start a second Jet node

Now, let's open another terminal window in the Jet installation folder and
start another Jet node, and see what happens:

<!--DOCUSAURUS_CODE_TABS-->
<!--Standalone-->
```bash
$ bin/jet-start
```
<!--Docker-->
```bash
$ docker run hazelcast/hazelcast-jet
```
<!--END_DOCUSAURUS_CODE_TABS-->


After a little time, you should some output similar to below:

<!--DOCUSAURUS_CODE_TABS-->
<!--Standalone-->
```text
Members {size:2, ver:2} [
    Member [192.168.0.2]:5701 - 7717160d-98fd-48cf-95c8-1cd2063763ff
    Member [192.168.0.2]:5702 - 5635b256-b6d5-4c88-bf45-200f6bf32104 this
]
```
<!--Docker-->
```text
Members {size:2, ver:2} [
	Member [172.17.0.2]:5701 - 4bc3691d-2575-452d-b9d9-335f177f6aff
	Member [172.17.0.3]:5701 - 7d07aad7-4a22-4086-a5a1-db64cf664e7d this
]
```
<!--END_DOCUSAURUS_CODE_TABS-->

Congratulations, now you have created a cluster of two nodes! 

>If for some reason your nodes didn't find each other, this is likely
>because multicast is turned off or not working correctly in your
>environment. In this case, please see the
>[Configuration](../operations/configuration) section as this will
>require you to use another cluster discovery mechanism than the
>default, which is multicast.

You will now see that the job is running both nodes automatically,
although you will still only see output on one of the nodes. This is
because the test data source is non-distributed, and we don't have any
steps in the pipeline which require data re-partionining. We will
explain this later in detail in the
[distributed computing](../concepts/distributed-computing) section.

Another thing you'll notice here is that the sequence numbers that are
outputted will have reset, this is because the test source we are using
is not fault-tolerant. This is explained in detail in the [fault
tolerance](concepts/fault-tolerance) section.

## Terminate one of the nodes

We can also the reverse, just terminate one of the nodes by kill the
processes and the job will restart automatically and keep running on the
remaining nodes.

## Next Steps

You've now successfully deployed and scaled your first distributed data
pipeline. The next step is getting a deeper understanding of the
[Pipeline API](../reference) which will allow you to write more complex
data transforms.
