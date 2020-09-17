---
title: Job Management
description: Commands to manage SQL jobs
---

## CREATE/DROP JOB

Creates a job from a query that is not tied to the client session. When
you submit an INSERT query, its lifecycle is tied to the client session:
if the client disconnects, the query is cancelled, even though it
doesn't deliver results to the client.

If you want to submit a statement that is independent from the client
session, use the `CREATE JOB` command. Such a query will return quickly
and the job will run on the cluster. It can also be configured to be
fault tolerant.

### CREATE JOB

<img src="/docs/assets/ddl-CreateJob.svg" style="display:inline-block"/>

**Options ::=**
<img src="/docs/assets/ddl-Options.svg" style="display:inline-block"/>

- `JobName`: a unique name identifying the job.

- `QuerySpec`: the query to run by the job. It must not return rows to
  the client, that is it must not start with `SELECT`. Currently we
  support `INSERT INTO` or `SINK INTO` queries.

- `OptionName`, `OptionValue`: the job configuration options. The list
 of options matches the methods in the `JobConfig` class.

#### Example

```sql
CREATE JOB myJob
OPTIONS (
    processingGuarantee 'exactlyOnce',
    snapshotIntervalMillis '5000',
    metricsEnabled 'true'
) AS
INSERT INTO my_sink_topic
SELECT * FROM my_source_topic
```

### DROP JOB

<img src="/docs/assets/ddl-DropJob.svg" style="display:inline-block"/>

- `IF EXISTS`: don't throw an error if the job doesn't exist

- `WITH SNAPSHOT`: export a named snapshot before cancelling the job
  (enterprise feature)

## Other job operations

These operations are equivalent to `Job.suspend()`, `Job.resume()` and
`Job.restart()`, see their javadoc for more information.

<img src="/docs/assets/ddl-AlterJob.svg" style="display:inline-block"/>

To export a snapshot for a running job use:

<img src="/docs/assets/ddl-CreateSnapshot.svg" style="display:inline-block"/>

- `OR REPLACE`: this option is ignored, Jet always replaces the snapshot
  if it already exists

- `SnapshotName`: the name of the saved snapshot

- `JobName`: the job for which to create the snapshot

The job will continue running after the snapshot is exported. To cancel
the job after the snapshot is exported, use the [DROP JOB .. WITH
SNAPSHOT](#drop-job) command.

To delete a previously exported snapshot use:

<img src="/docs/assets/ddl-DropSnapshot.svg" style="display:inline-block"/>

- `IF EXISTS`: don't throw an error if the snapshot doesn't exist

- `JobName`: the job for which to create the snapshot

To start a new job using the exported snapshot as initial, use [CREATE
JOB](#create-job) command with the `InitialSnapshot` option set to the
snapshot name.

*Note:* Exported snapshots is an enterprise feature.
