---
title: Submit the job to the cluster
id: submit-job
---

The next step is to learn how to submit the pipeline that we created
earlier. Once you have a Jet node up and running, it's now time to
package your application and submit it as a job to the cluster.

## Use the Bootstrapped Instance

In the previous section, we used `Jet.newJetInstance()` to create a
full Jet node as part of our application and submitted the job to it. In
practice you'll want to submit a job to a separate, standalone cluster.

To achieve this, Jet uses the concept of a _bootstrapped instance_ which
acts differently depending on which context it's called from.

Now change the line:

```java
JetInstance jet = Jet.newJetInstance();
```

to

```java
JetInstance jet = Jet.bootstrappedInstance();
```

If you run the application normally, this will create a single, isolated
node whereas when used with the `jet` command line tool it will instead
create a Jet client which will submit your job to the cluster.
However before doing this we must package our application.

## Package the job as a JAR

You can use a Java build system such as [Apache
Maven](https://maven.apache.org) or [Gradle](https://gradle.org) to
package your application as a single JAR.

For example, if you're using Maven, the `mvn package` command will
generate a JAR file which is now ready to be submitted to the cluster.
You can also make the JAR runnable by default using the `Main-Class` attribute
in `MANIFEST.MF`.

## Submit to the cluster

From the Jet home folder, now execute the command to submit the job to the
cluster:

<!--DOCUSAURUS_CODE_TABS-->
<!--Standalone-->
```bash
$ bin/jet submit <path_to_JAR_file>
```
<!--Docker-->
```bash
$ docker run -it -v <path_to_JAR_file>:/jars hazelcast/hazelcast-jet jet -a 172.17.0.2 submit /jars/<name_of_the_JAR_file>
```
<!--END_DOCUSAURUS_CODE_TABS-->


If you didn't specify a main class, you can use the `-c` option to specify
the class name:

<!--DOCUSAURUS_CODE_TABS-->
<!--Standalone-->
```bash
$ bin/jet submit -c <main_class_name> <path_to_JAR_file>
```
<!--Docker-->
```bash
$ docker run -it -v <path_to_JAR_file>:/jars hazelcast/hazelcast-jet jet -a 172.17.0.2 submit -c <main_class_name> /jars/<name_of_the_JAR_file>
```
<!--END_DOCUSAURUS_CODE_TABS-->

You will now notice on the server logs that a new job has been submitted
and it's running on the cluster. The output of the job, now appears on
the logs of the server rather than on the client. This is an interesting
caveat of [distributed computation](concepts/distributed-computing)
which we will delve into later on.

You should now see the job as running and printing output on the server.
You can also see a list of running jobs as follows:

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

Notice that even if you kill the client application, the job is still running
on the server. For streaming jobs, a job will run indefinitely until explicitly
cancelled (`jet cancel <job-id>`) or the cluster is shut down. Keep the job
running for now, as the next steps will be scaling it up or down.
