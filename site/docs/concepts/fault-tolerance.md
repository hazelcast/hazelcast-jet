---
title: Elasticity
id: elasticity
---

## Fault Tolerance and Processing Guarantees

One less-than-obvious consequence of stepping up from finite to infinite
streams is the difficulty of forever maintaining the continuity of the
output, even in the face of changing cluster topology. A Jet node may
leave the cluster due to an internal error, loss of networking, or
deliberate shutdown for maintenance. This will cause the computation job
to be suspended. Except for the obvious problem of new data pouring in
while we're down, we have a much more fiddly issue of restarting the
computation in a differently laid-out cluster exactly where it left off
and neither miss anything nor process it twice. The technical term for
this is the "exactly-once processing guarantee".

Jet achieves fault tolerance in streaming jobs by making a snapshot of
the internal processing state at regular intervals. If a member of the
cluster fails while a job is running, Jet will detect this and restart
the job on the new cluster topology. It will restore its internal state
from the snapshot and tell the source to start sending data from the
last "`committed`" position (where the snapshot was taken). The data
source must have built-in support to replay the data from the given
checkpoint. The sink must either support transactions or be
_idempotent_, tolerating duplicate submission of data.

In a Jet cluster, one member is the _coordinator_. It tells other
members what to do and they report to it any status changes. The
coordinator may fail and the cluster will automatically re-elect another
one. If any other member fails, the coordinator restarts the job on the
remaining members.