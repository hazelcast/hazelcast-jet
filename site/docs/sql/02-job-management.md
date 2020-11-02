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

### CREATE JOB Synopsis

```sql
CREATE JOB [IF NOT EXISTS] job_name
[ OPTIONS ( option_name 'option_value' [, ...] ) ]
AS query_spec
```

- `job_name`: a unique name identifying the job.

- `query_spec`: the query to run by the job. It must not return rows to
  the client, that is it must not start with `SELECT`. Currently we
  support `INSERT INTO` or `SINK INTO` queries.

- `option_name`, `option_value`: the job configuration options. The list
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

### DROP JOB Synopsis

```sql
DROP JOB [IF EXISTS] job_name [WITH SNAPSHOT snapshot_name]
```

- `IF EXISTS`: don't throw an error if the job doesn't exist

- `WITH SNAPSHOT`: export a named snapshot before cancelling the job
  (enterprise feature)

## Other job operations

These operations are equivalent to `Job.suspend()`, `Job.resume()` and
`Job.restart()`, see their javadoc for more information.

```sql
ALTER JOB job_name { SUSPEND | RESUME | RESTART }
```

To export a snapshot for a running job use:

```sql
CREATE [OR REPLACE] SNAPSHOT snapshot_name FOR JOB job_name
```

- `OR REPLACE`: this option is ignored, Jet always replaces the snapshot
  if it already exists

- `snapshot_name`: the name of the saved snapshot

- `job_name`: the job for which to create the snapshot

The job will continue running after the snapshot is exported. To cancel
the job after the snapshot is exported, use the [DROP JOB .. WITH
SNAPSHOT](#drop-job) command [described above](#drop-job).

To delete a previously exported snapshot use:

```sql
DROP SNAPSHOT [IF EXISTS] snapshot_name
```

- `IF EXISTS`: don't throw an error if the snapshot doesn't exist

- `job_name`: the job for which to create the snapshot

To start a new job using the exported snapshot as initial, use [CREATE
JOB](#create-job) command with the `initialSnapshotName` option set to
the snapshot name.

CREATE MAPPING Syntax*Note:* Exported snapshots is an enterprise feature.
