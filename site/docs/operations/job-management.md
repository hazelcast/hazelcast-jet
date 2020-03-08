---
title: Job Management
description: How to manipulate the lifecycle of processing jobs in Jet.
---

## Submitting Jobs

```bash
$ bin/jet submit -n hello-world examples/hello-world.jar
Submitting JAR 'examples/hello-world.jar' with arguments []
Using job name 'hello-world'
```

## Listing Jobs

```bash
bin/jet list-jobs
```

```bash
ID                  STATUS             SUBMISSION TIME         NAME
0401-9f77-b9c0-0001 RUNNING            2020-03-07T15:59:49.234 hello-world
```

## Cancelling Jobs

```bash
bin/jet cancel <job_name_or_id>
```

## Restarting

```bash
bin/jet restart <job_name_or_id>
```

## Suspending and Resuming

```bash
$ bin/jet suspend hello-world
Suspending job id=0401-9f77-b9c0-0001, name=hello-world, submissionTime=2020-03-07T15:59:49.234...
Job suspended.
```

```bash
$ bin/jet resume hello-world
Resuming job id=0401-9f77-b9c0-0001, name=hello-world, submissionTime=2020-03-07T15:59:49.234...
Job resumed.
```
