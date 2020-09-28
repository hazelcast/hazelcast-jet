---
title: Error Handling Strategies
description: Ways of coping with unexpected error in Hazelcast Jet.
---

There is unavoidable risk in running Jet jobs. They ingest data from
external sources, the correctness and consistency of which can't be
guaranteed. The processing they perform includes the running of user
code, which is unavoidably imperfect.

So even if we assume no bugs in the Jet framework itself, errors can and
will happen. What options Jet offers for dealing with them is the topic
of the sections below.

## Scope

A Jet cluster, at any given time, is running an arbitrary number of
independent jobs in parallel. These jobs do not interact in any way, and
the failure of one should not lead to any consequences for any other.
When considering error handling, the basic unit of discussion will be
the job, mainly what happens with it once it encounters a problem.

The default way Jet reacts to an error is to fail the job that has
produced it, but what one can do with such a failed job afterward is the
interesting part.

## Types of Errors

Before we dig deeper, let's list some typical error one might encounter:

* *input data errors*: unexpected input data, e.g. unexpected null
  value, out-of-range date (`0000-00-00`) or malformed JSON document
* *coding errors*: e.g., a user-provided filtering lambda throws a
  `NullPointerException` for certain input or stateful mapping code
  contains memory leaks
* *I/O errors*: network errors or remote service failures

## Specific Solutions

Some concrete types of error have solutions specifically tailored to
address them.

### In-Memory Data Structures

Jet comes out of the box with some [in-memory distributed data
structures](data-structures) which can be used as a data source or a
sink. These are hosted by Hazelcast clusters, which can be the same as
the one Jet runs on or separate, remote ones.

In this case, a large portion of error handling (for example,
communications failures with the remote cluster, but not just those) is
handled transparently to Jet, by the Hazelcast cluster itself. For
further details and configuration options, read the [relevant section in
the Hazelcast
manual](https://docs.hazelcast.org/docs/latest/manual/html-single/#handling-failures).

### Individual Sources & Sinks

Sources and Sinks are Jet's points of contact with external data stores.
Their various types have specific characteristics that enable error
handling strategies applicable only to them.

For example, our [Change Data Capture
sources](sources-sinks#change-data-capture-cdc) can attempt to reconnect
automatically whenever they lose connection to the databases they
monitor, so for intermittent network failures, their owner jobs don't
need to fail.

To see the failure handling options of a given [source or
sink](sources-sinks), consult its Javadoc.

## Generic Solutions

In Jet we also have generic options for dealing with random errors
affecting our jobs.

### Jobs Without Mutable State

For many streaming jobs, specifically the ones which don't have any
processing guarantee configured, the pipeline definition and the job
config are the only parts we can identify as state, and those are
immutable.

Batch jobs also fall into the category of jobs with immutable state.

One option for dealing with failure in immutable-state jobs is simply
restarting them (once the cause of the failure has been addressed).
Restarted streaming jobs lacking mutable state can just resume
processing the input data flow from the current point in time, batch
jobs can be re-run from the beginning.

### Processing Guarantees

Jobs with mutable state, those with a [processing
guarantee](../architecture/fault-tolerance#processing-guarantee-is-a-shared-concern)
set, achieve fault tolerance by periodically saving [recovery
snapshots](../architecture/fault-tolerance#distributed-snapshot). When
such a job fails, not only does its execution stop, but also its
snapshots get deleted. This makes it impossible to resume it without
loss.

To cope with this situation, you can configure a job to be suspended in
the case of a failure, instead of failing completely. For details see
[`JobConfig.setSuspendOnFailure`](/javadoc/{jet-version}/com/hazelcast/jet/config/JobConfig.html#setSuspendOnFailure(boolean))
and
[`Job.getSuspensionCause`](/javadoc/{jet-version}/com/hazelcast/jet/Job.html#getSuspensionCause()).

Note: this option was introduced in Hazelcast Jet 4.3. In order to
preserve the behavior from older versions, it is not the default, so it
must be enabled explicitly.

A job in the suspended state has preserved its snapshot, and you can
resume it without loss once you have addressed the root cause of the
failure.

In the open-source version of Jet this scenario is limited to fixing the
input data by some external means and then simply [resuming the
job](../operations/job-management#restarting) via the client API.

The Enterprise version of Jet  has the added option of [job
upgrades](../enterprise/job-update). In that case you can:

* export the latest snapshot
* update the pipeline, if needed, for example, to cope with unexpected
  data
* resubmit a new job based on the exported snapshot and the updated
  pipeline

One caveat of the suspend-on-failure feature is that the latest snapshot
is not a "failure snapshot." Jet can't take a full snapshot right at the
moment of the failure, because its processors can produce accurate
snapshots only when in a healthy state. Instead, Jet simply keeps the
latest periodic snapshot it created. Even so, the recovery procedure
preserves the at-least-once guarantee.
