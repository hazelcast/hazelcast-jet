---
title: Job Management
description: How to manage lifecycle of processing jobs in Jet.
---

## Submitting Jobs

You can submit jobs to a cluster using the `jet submit` command
and packaging the job as a JAR:

```bash
$ bin/jet submit -n hello-world examples/hello-world.jar arg1 arg2
Submitting JAR 'examples/hello-world.jar' with arguments [arg1, arg2]
Using job name 'hello-world'
```

For a full guide on submitting jobs, see the relevant section in
[Programming Guide](../api/submitting-jobs).

## Listing Jobs

You can use the Jet command line to get a list of all runnings jobs
in the cluster:

```bash
$ bin/jet list-jobs
ID                  STATUS             SUBMISSION TIME         NAME
0401-9f77-b9c0-0001 RUNNING            2020-03-07T15:59:49.234 hello-world
```

You can also see completed jobs, by specifying the `-a` parameter:

```bash
$ bin/jet list-jobs -a
ID                  STATUS             SUBMISSION TIME         NAME
0402-de9d-35c0-0001 RUNNING            2020-03-08T15:14:11.439 hello-world-v2
0402-de21-7f00-0001 FAILED             2020-03-08T15:12:04.893 hello-world
```

## Cancelling Jobs

A streaming Jet job will run indefinitely until cancelled. You can cancel
a job as follows:

```bash
bin/jet cancel <job_name_or_id>
```

```bash
$ bin/jet cancel hello-world
Cancelling job id=0402-de21-7f00-0001, name=hello-world, submissionTime=2020-03-08T15:12:04.893
Job cancelled.
```

Once a job is cancelled, the snapshot for the job is lost and the job
can't be resumed. Cancelled jobs will have the "failed" status. Only
batch jobs are able to complete successfully.

## Suspending and Resuming

Jet supports suspending and resuming of streaming jobs in a fault-tolerant
way. The Job must be configured with [a processing guarantee](../api/submitting-jobs#setting-processing-guarantees)

```bash
bin/jet suspend <job_name_or_id>
```

```bash
$ bin/jet suspend hello-world
Suspending job id=0401-9f77-b9c0-0001, name=hello-world, submissionTime=2020-03-07T15:59:49.234...
Job suspended.
```

```bash
bin/jet resume <job_name_or_id>
```

```bash
$ bin/jet resume hello-world
Resuming job id=0401-9f77-b9c0-0001, name=hello-world, submissionTime=2020-03-07T15:59:49.234...
Job resumed.
```

## Restarting

```bash
bin/jet restart <job_name_or_id>
```
