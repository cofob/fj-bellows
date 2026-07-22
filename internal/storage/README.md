# internal/storage

`storage` is fj-bellows' durable recovery and accounting boundary. Provider
APIs and ownership tags remain authoritative for whether a cloud resource
exists; SQLite records the daemon's intent and enough identity to reconcile
provider state safely after a restart.

The package stores:

- provider resources and immutable billing anchors;
- clean filesystem generations across in-place rebuilds;
- open and completed lifecycle phases;
- Forgejo jobs, workflow identity, outcomes, and timing sources;
- managed snapshot build/rotation state;
- immutable catalog quotes and fixed-point cost estimates; and
- durable route assignments, replay payloads, and immutable candidate
  scorecards; and
- provider mutation intent written before external side effects.

All money values are signed 64-bit integer nanounits of an ISO currency. An
unknown price is persisted as SQL `NULL` with `known = 0`; it is never reported
as zero. Statistics keep currencies separate and expose both known and unknown
coverage.

## SQLite operation

Open a database with `storage.Open`. The deployment must create and persist the
parent directory; the package intentionally does not create it. SQLite runs
with WAL journaling, foreign keys, a busy timeout, and `synchronous=FULL`.
Embedded forward-only migrations run in one transaction. A binary refuses to
open a database whose schema version is newer than it understands. New ledgers
are created with mode `0600`; existing paths that are not regular private files
are rejected.

`DashboardStore` is a separate read-only projection of live `observed` jobs.
It left-joins routing decisions so the web UI can distinguish explicit queue
work, undecided automatic work, and jobs intentionally held behind an already
paid worker without widening the orchestrator's `Store` interface.

```go
db, err := storage.Open(ctx, "/var/lib/fj-bellows/fj-bellows.db")
if err != nil {
    return err
}
defer db.Close()
```

`BeginResource`, `BeginSnapshot`, and `BeginMutation` persist intent before a
provider request. `OpenResources`, `OpenGenerations`, `OpenPhases`,
`OpenJobs`, `Snapshots`, and `PendingMutations` are the startup recovery
surface. History pagination uses an opaque stable cursor ordered by immutable
first-observation time.

Statistics aggregate served jobs by workflow, tier, provider, or UTC day;
unclaimed queue observations stay visible in history but do not skew run
counts. Queue, dispatch, run, and fleet phase timings use exact nearest-rank
percentiles. Fleet phase and cost totals remain separate from workflow-direct
compute so warm-pool and image-builder overhead is not misattributed. Active
VM compute, open lifecycle phases, and active snapshot storage are accrued at
report time without checkpoint writes. Reporting windows clip overlapping
intervals and split them at UTC day boundaries. Terminal records are
replay-idempotent, so recovery after a committed cost write cannot double
charge; deletion/teardown seals each virtual interval as immutable history.
Routing effectiveness reports decisions, completion/P95 outcomes,
fallback/history/idle selection, chosen-tier distribution, normalized
estimated and actual direct costs, savings against the fallback, deferrals,
and unknown-price coverage. Routing APIs live on the separate `RoutingStore`
interface so ordinary orchestrator fakes remain small.

Retention is explicit. A zero cutoff is a no-op, and cleanup never removes
open resources, generations, phases, jobs, snapshot work, or pending mutation
intent. SQLite is designed for one fj-bellows daemon on a local persistent
filesystem; shared network filesystems and multiple writers are unsupported.
