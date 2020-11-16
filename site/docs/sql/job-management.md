---
title: Job Management
description: Commands to manage SQL jobs
---

## CREATE/DROP/ALTER JOB

These statements create a potentially long-running Jet job that is not
tied to the client session.

When you submit a standard INSERT query, its lifecycle is tied to the
client session: if the client disconnects, the query is cancelled, even
if it is supposed deliver results somewhere else (not back to the
client).

If you want to submit a statement that is independent from the client
session, use the `CREATE JOB` statement. Such a statement will complete
quickly and let the job running in the cluster. You can also configure
it to be fault-tolerant.

### CREATE JOB Synopsis

```sql
CREATE JOB [IF NOT EXISTS] job_name
[ OPTIONS ( option_name 'option_value' [, ...] ) ]
AS query_spec
```

- `job_name`: a unique name identifying the job.

- `query_spec`: the query the job will run. Currently we support `INSERT
  INTO` and `SINK INTO` queries. `SELECT` is not supported by design
  because it returns the results to the client.

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

- `IF EXISTS`: return silently if the job doesn't exist

- `WITH SNAPSHOT`: export a named snapshot before cancelling the job
  (Enterprise feature)

### ALTER JOB Synopsis

```sql
ALTER JOB job_name { SUSPEND | RESUME | RESTART }
```

Get more details on Jet job management in the [Operations
Guide](/docs/operations/job-management).

## CREATE/DROP SNAPSHOT

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
SNAPSHOT](#drop-job-synopsis) command described above.

To delete a previously exported snapshot use:

```sql
DROP SNAPSHOT [IF EXISTS] snapshot_name
```

- `IF EXISTS`: don't throw an error if the snapshot doesn't exist

- `job_name`: the job for which to create the snapshot

To start a new job using the exported snapshot as the starting point,
use the [CREATE JOB](#create-job-synopsis) command with the
`initialSnapshotName` option set to the snapshot name.

*Note:* Exported snapshots are an Enterprise feature.
