---
title: 012 - Improved Error Tolerance of Streaming Jobs
description: Make stream jobs capable of surviving errors with fixable causes.
---

*Target Release*: 4.3

## Goal

Since streaming jobs can't simply be re-run, like batch ones, having
them fail can result in significant loss. We want them to be as
resilient as possible and only ever really fail due to the most severe
of causes.

Right now any kind of failure, like exceptions in user code or thrown
by sources or sinks will stop the execution of a job. Moreover, not only
does execution stop, snapshots of the job are also lost, so even if
the root problem can be fixed, there is no way to recover or resume
the failed job.

We want to improve on this by only suspending jobs with such failures
and preserving their snapshots.

## Breaking-change

It might be argued that suspending a job on failure, instead of letting
it fail completely is a breaking-change, as far as behaviour is
concerned.

One might also argue that it's broken behaviour which just took a long
time to fix.

Anyways, to be safe we might preserve the current behaviour as default
and make the suggested changes optional. Maybe as a new element in
`JobConfig`, called `suspend_on_failure`, disabled unless otherwise
specified.

## Notify client

When we suspend a job due to a failure we need to notify the client that
submitted it and give enough information to facilitate the repair of the
underlying root cause.

For this reason we should introduce a new `JobStatus`, called
`SUSPENDED_DUE_TO_ERROR` and provide details in the `Exception` set in
the `CompletableFuture` returned by `job.getFuture()` (also used by
`job.join()`).

Since we will use a new state we need to make sure that it behaves
exactly like the regular suspend: the job can be cancelled, snapshots
can be exported (enterprise feature) and so on.

## When to use

Do we always want to suspend jobs when there is a failure? It doesn't
make sense to do it for failures that can't be remedied, but which are
those? Hard to tell, hard to exhaust all the possibilities.

Since the suspend feature will be optional and will need explicit
setting, it's probably ok to do it for any failure (not just some
whitelisted ones). There is nothing to loose anyways. If the failure
can't be remedied, then the user can just cancel the job. They will be
no worse off than if it had failed automatically.

## Processing guarantees

Should we enable this feature only for jobs with processing guarantees?
The answer is probably yes. Jobs without a processing guarantee don't
save snapshots, so there's not much state to loose. But is this true? Do
jobs have state outside snapshots?

Plus there is not much to gain by not allowing this feature for
"stateless" jobs... Maybe we should not discriminate on these grounds.

## Batch jobs

Batch jobs can always just be restarted, as the mantra goes. But still,
they can take a long time to restart, so we probably should allow them
to work in this way too.

As with other aspects mentioned above we don't gain anything by
disabling this functionality for batch jobs.

## Job upgrade

Even if we enable this feature some fixable problems won't be actually
possible to handle in open-source Jet, because they might require
pipeline code change and that will need snapshot export and job upgrades
which are enterprise features.

Not sure what we can do about this, besides being aware of it.

## Failure snapshot

Ideally, when an error happens, which will be handled by suspending the
job, we would prefer to make all processors take a snapshot right then
so that we can later resume execution from the most advanced state. But
this, unfortunately doesn't seem possible.

Snapshots can be taken only when all processors are functioning properly
(due to the nature of how distributed snapshots happen).

But, even some slightly obsolete snapshot should be better than loosing
the whole computation, so I guess this is a weakness we can live with.

## In-processor error handling

This functionality should be a solution of last resort, meaning that all
errors that can be handled without user intervention should be handled
automatically. For example sources and sinks loosing connection to
external systems should attempt reconnection internally, attempt a
back-off after a certain number of tries, in general have their internal
strategy for dealing with problems as much as possible.
