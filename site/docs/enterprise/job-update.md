---
title: Updating Jobs
description: Update Streaming Jobs without losing computation state
---

A streaming job can be running for days or months without disruptions
and occasionally it may be necessary to update the job's pipeline while
at the same preserving its state.

For example, imagine a streaming job which does a windowed aggregation
over a 24-hour window, and due to new business requirements, an
additional need is that an additional 1-hour window must be added to
the pipeline and some additional filter needs to be added.

While implementing this requirement, we don't want to lose the current
computational state because it includes data for the past day and in
many cases it may not be able to replay this data fully.

## Updating The Job

### Updating The Pipeline

The first step you must when updating a job is to update its pipeline.
The new pipeline must be _state-compatible_ with the old one, meaning
that new pipeline must be able to work with the previous state.

The following general points must be followed:

* You can add new [stateful stages](../api/stateful-transforms) and
  remove existing ones without breaking compatibility. This includes
  adding/removing a source or sink or adding a new aggregation path from
  existing sources.
* You can freely add, remove or change [stateless stages](../api/stateless-transforms),
  such as `filter`/`map`/`flatMap` stages, non-transactional sinks among
  others.
* The stage names must be preserved between job versions - naming stages
  explicitly is suggested.

For additional details, please see the [state compatibility guide](#state-compatibility).

### Exporting A Snapshot

To update a job, the first we need to do is to take a snapshot of its
current state. This can be achieved by using the following command:

```bash
bin/jet save-snapshot <job name or id> <snapshot name>
```

This will take an in-memory snapshot of the job while keeping it running
and save it with the specified name. You can see a list of the current
exported snapshots in the cluster with the following command:

```bash
$ bin/jet list-snapshots
TIME                    SIZE (bytes)    JOB NAME                 SNAPSHOT NAME
2020-03-15T14:37:01.011 1,196           hello-world              snapshot-v1
```

You will note that the job where the snapshot was exported from is still
running. You can optionally atomically take a snapshot and cancel the job
as follows:

```bash
bin/jet save-snapshot -C <job name or id> <snapshot name>
```

### Starting the Updated Job

When submitting a job, it's possible to specify an initial snapshot to
use. The job then will start at the specified snapshot, and as long as
state-compatibility is maintained, will continue running once the
snapshot is restored. To submit a job starting from a specific snapshot
you can use the following command:

```bash
bin/jet submit -s snapshot-v1 <jar name>
```

## Memory Requirements

Internally, Jet stores these snapshots in an `IMap`, that are separate
from the periodic snapshots that are taken as part of the job execution.
Exporting snapshots requires enough available memory in the cluster to
store the computation state.

## State Compatibility

The state has to be compatible with the updated pipeline. As a Jet
pipeline is converted to a [DAG](../architecture/distributed-computing),
the snapshot contains separate data for each vertex, identified by the
transform name. The stateful transforms in previous and updated pipeline
must have the same name for the state to be restored successfully. Once
the job is started again from a snapshot, the following rules are
applied:

* If a transform was not in the previous version and is available in the
  new version, the transform will be restored with an empty state.
* If a transform was in the previous version, and not in the new
  version, then its state will simply be ignored.
* If the transform exited in the previous version and exists in the new
  version as well and their names much, then the state from the previous
  version will be restored as the state of the new transform.

Using these rules, the following are possible:

* you can add and new stateful stages and remove existing ones without
  breaking compatibility. This includes adding/removing a source or sink
  or adding a new aggregation path from existing sources.
* you can freely add, remove or change stateless stages, such as
  filter/map/flatMap stages, sinks and others

You can also find additional information about what state is stored
under the Javadoc for each transform. Here are some examples of other
supported changes:

* adding new sinks to existing pipeline stages
* adding new branches to existing pipeline stages
* change session window timeout
* change connection parameters of sources/sinks
* enable early results for a window
* for sliding windows, you can increase or reduce the window size while
  keeping the step size the same.
* increase eviction timeout for stateful map
* change parameters of aggregate operation: for example, change the
  comparator of `AggregateOperation.minBy()`
* tweak the aggregate operation, but accumulator type has to stay the
  same
* any change to stateless stages, for example updating a service in
  `mapUsingService` or updating the python module in `mapUsingPython`
* renaming a stateless stage

The following changes are not supported:

* change a sliding window to a session window
* change size of a tumbling window to another size which is not a direct
  multiple of the previous size
* replace aggregation operation for another one with a different
  accumulator
* rename a stateful stage

## Upgrading Between Jet Versions

You can also use the job update feature to update between Jet patch
versions. We eventually also plan to support updates between minor
versions, and currently this may or may not work depending on what
transforms are updated. Please note that you need to use [lossless
restart](lossless-restart) to update the Jet version without losing the
cluster state as the whole cluster needs to be restarted.
