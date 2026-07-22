# fj-bellows design

## Goal

Run Forgejo Actions jobs on ephemeral cloud runners, support several machine
classes/providers in one daemon, preserve a clean one-job boundary when
requested, and account durably for lifecycle timing and cost. Provider billing
semantics remain provider attributes; the core contains no cloud-specific
configuration.

## Configuration and fleet shape

The configuration is tiers-only:

- `providers` declares named provider instances as `{driver, config}`. The core
  retains each `config` as an opaque `yaml.Node`; the selected driver decodes it.
- `tiers` declares independent pools. Each tier owns one `required_label`, a
  provider reference, `instance_type`, advertised labels, `max_instances`, an
  optional proactive `warm_instances` reserve, and isolation/reset timers.
- `database.path` names the required persistent SQLite ledger. Its parent must
  already exist. A zero `database.retention` keeps history forever.
- `routing.routes` optionally declares fleet-level automatic labels and their
  candidate tiers. Fixed exchange rates normalize provider quotes into one
  configured score currency.
- The deployment tag and tier name are length-delimited and hashed into one
  provider-safe ownership tag. This avoids ambiguous joins and overlong cloud
  tags. Separate deployments sharing an account must use distinct tags.

Configuration rejects duplicate required labels and a tier advertising another
tier's required label. This makes routing deterministic even though each tier
polls Forgejo independently. The fleet coordinator runs one orchestrator per
tier and aggregates control-plane state; provisioning decisions remain inside
each tier's single-writer reconcile loop.

## Automatic cost-aware routing

Automatic routing is a fleet coordinator, not a provider driver. It polls each
automatic Forgejo label once and evaluates jobs sequentially so all routes
share virtual capacity reservations during a polling pass. Explicit tier
labels bypass it. Candidate tiers advertise the automatic label, but their
wrapped job sources suppress automatic-only queue results until a durable
SQLite assignment exists.

The profile identity is Forgejo source, repository ID, workflow ID/file, and
job name. Succeeded and normally failed attempts contribute runtime samples;
cancelled, skipped, infrastructure-failed, and interrupted attempts do not.
Before `min_samples` (default 10), the configured fallback wins when it is
available. Otherwise global workflow-job P95 is used, with tier-specific P95
once that tier independently has enough samples. `cold_start_p95` defaults to
15 minutes and `history_window` to 720 hours.

Eligible tiers must be healthy, label-compatible, and have either an idle
worker or provisioning capacity. Scores are normalized marginal cost from the
provider's `Pricer` quote. Cold scores include provisioning P95, predicted run
P95, reset P95 when applicable, billing quantum/minimum charge/minimum
duration, and calendar-month caps. Idle scores are the increase in billed
allocation cost through predicted completion; reuse inside an already-paid
hour can score zero. Deterministic ties prefer idle capacity, shorter startup,
configured candidate order, then tier name.

Routes may set `max_optimization_wait_queue` (default `0`). After a job reserves
an idle hourly worker, the router can schedule that many later jobs behind the
same paid allocation when their run and reset P95s still fit before its reap
boundary. Those exact handles remain in the selected tier queue but do not
create new capacity. The next job beyond the bound is scored against normal
cold capacity. Worker IDs and scheduled start/finish times are durable, so a
restart reconstructs the reservation; a vanished worker or expired window
releases the hold and lets the tier provision normally.

The decision and every candidate scorecard are committed before the selected
tier sees the job. The stored queue payload and renewable unclaimed lease make
assignments restart-safe without keeping vanished queue work forever. Reports
retain chosen-tier distribution, fallback/history/idle/P95 outcomes, estimated
savings against the fallback, normalized actual direct cost, and unknown-price
coverage. `route_decided` and `route_deferred` join the normal event stream;
router poll freshness and cached-price degradation join health.

## Billing, warm capacity, and isolation

The provider declares one of two billing models:

- **Per second**: an idle worker is deleted after the tier's `idle_timeout`.
- **Hourly round-up**: an idle worker is deleted at
  `CreatedAt + N*billing_hour - hour_margin`. With the defaults, that is just
  before each paid-hour boundary (the `:55` rule). `CreatedAt` always comes
  from the provider and survives daemon restarts and Hetzner rebuilds.

`warm_instances` is a minimum ready-idle reserve. Busy workers do not satisfy
it; provisioning and resetting workers count as future capacity. The reserve
is orthogonal to billing retention: zero disables proactive creation, while an
hourly worker created for demand may still remain until its paid boundary. A
positive reserve is replenished after teardown and therefore intentionally
keeps that many workers across billing boundaries.

`one_job_per_vm` selects the post-attempt cleanliness contract:

- `false`: return the worker directly to Idle and let the billing policy reap
  it later.
- `true` with `reset_mode: none`: delete it after every attempt, including
  registration/runner failures, then refill demand or the warm reserve.
- `true` with `reset_mode: snapshot`: use the provider's managed-image/reset
  capabilities. This mode is currently implemented by Hetzner.

All workers, in-flight creates, resets, and golden-image builders count toward
`max_instances`. Queued jobs take precedence over starting a builder.

## Hetzner golden snapshot reset

Each snapshot tier identifies its desired image with a deterministic
fingerprint covering the tier, driver, instance type, provider configuration,
runner/agent versions, and a versioned marker over the embedded bootstrap and
sysprep contract.

When no matching owned image exists, a tier with `warm_instances > 0` starts a
builder eagerly. A scale-to-zero tier starts it on first demand. The
orchestrator then:

1. Reserves one capacity slot and provisions a role-tagged builder.
2. Installs the static worker dependencies and waits for readiness.
3. Runs sysprep synchronously over the authenticated dispatch channel. Sysprep removes
   cloud-init state, machine identity, readiness markers, authorization/host
   keys, logs, and temporary agent material, then flushes the filesystem.
4. Gracefully powers the guest off through the provider, snapshots the root
   disk, and activates the new fingerprint.
5. Marks the builder generation unclean durably, promotes its provider role to
   worker, and rebuilds that same paid allocation from the new snapshot with
   fresh runtime secrets and a new host key. Only a failed hand-off is deleted.
6. Removes stale daemon-owned snapshots once no reset references them.

Builders and images use distinct ownership roles so normal worker listing can
never adopt an in-progress build. Promotion happens only after durable state
has made the scrubbed generation undispatchable; a crash during hand-off can
therefore only drain the VM. The snapshot carries no Forgejo registration
token, provider token, deployment SSH material, or job state. Attached volumes
are outside the snapshot lifecycle.

With a single-slot cold tier, the queued job waits for the initial image and is
then served by the promoted builder; no second hourly minimum is incurred.
Larger tiers may serve concurrent cold capacity while the reserved build slot
runs. Once active, new workers boot from the image. After each attempt, the
existing Hetzner server enters Resetting and is rebuilt from the golden
snapshot with fresh runtime cloud-init and a new pinned SSH host key.
Server ID, IP, network attachments, and provider `CreatedAt` remain the billing
allocation's identity. Reset/readiness failure deletes the server. A reset is
also skipped when less than `reset_min_remaining` remains before the reap mark,
avoiding reset work for a server about to be deleted.

## Durable SQLite boundary

Provider APIs and ownership tags are authoritative for whether a live cloud
resource exists. SQLite is authoritative for daemon intent and history. The
database uses WAL, foreign keys, a busy timeout, `synchronous=FULL`, and
transactional forward-only embedded migrations. A database written by a newer
schema is rejected.

The storage interface records:

- provider resources and immutable billing anchors;
- clean filesystem generations and lifecycle phase intervals;
- mutation intent before provision, reset, snapshot, and destroy side effects;
- observed jobs, attempts, workflow/repository identity, outcomes, and timing
  sources;
- managed snapshot state and fingerprints; and
- immutable provider price quotes and fixed-point cost entries.
- durable route assignments and immutable candidate scorecards.

Money is stored as signed 64-bit nanounits with an ISO currency; unknown prices
remain unknown rather than becoming zero, and statistics never combine
currencies. Job history supports stable cursor pagination. Statistics can group
by workflow, tier, provider, or UTC day and distinguish direct job compute from
warm/boot/reset/builder/snapshot overhead and incomplete price coverage.

The control listener also exposes `/dashboard/`. Its read-only JSON snapshot
joins live fleet health/workers with a SQLite queue projection, recent jobs,
and the same statistics aggregates used by `fjbctl`. The UI polls every five
seconds; it never mutates orchestrator state. With bearer auth configured, only
the data endpoint is protected while the data-free static shell remains public
and supplies the token from per-tab `sessionStorage`.
The browser also subscribes to `/dashboard/ws`, which bridges the existing
bounded fleet event bus. State-transition events debounce an immediate snapshot
read; periodic polling remains a fallback for disconnects and non-eventful
accounting changes. Browser WebSockets cannot set `Authorization`, so the token
is base64url-encoded in a requested subprotocol and never appears in the URL.

At startup, process-local open jobs/phases are marked interrupted. Each tier
then reconciles open ledger records and pending mutations with provider-tagged
ground truth: it adopts matching cloud resources missing locally and closes
records for resources that vanished. Retention deletes only completed history;
open resources, jobs, snapshots, and pending mutation intent are protected.

## Components

| Package | Responsibility |
| --- | --- |
| `internal/config` | tiers-only YAML, defaults/validation, opaque named-provider config |
| `internal/forgejo` | poll tier-filtered jobs, mint ephemeral runner registrations, enrich job identity |
| `internal/provider` | core provider contract, registry, billing model, optional reset/image/pricing capabilities |
| `internal/provider/digitalocean` | per-second Droplets and Sizes catalog pricing |
| `internal/provider/hetzner` | hourly servers, managed snapshots, rebuild reset, catalog pricing |
| `internal/provider/linode` | hourly Linodes and optional managed network/cache resources |
| `internal/storage` | SQLite recovery, history, statistics, and accounting |
| `internal/router` | fleet-level automatic queue coordination and marginal-cost scoring |
| `internal/bootstrap` | provider-neutral worker/builder cloud-init |
| `internal/orchestrator` | tier state machine, reconcile, warm scaling, teardown, reset, dispatch |
| `cmd/fj-bellows` | provider/tier composition, database lifecycle, flags, lock, control plane |

## Provider contracts

```go
type Provider interface {
    Configure(ctx context.Context, tag string, config yaml.Node) error
    Provision(ctx context.Context, spec Spec) (Instance, error)
    Destroy(ctx context.Context, id string) error
    List(ctx context.Context, tag string) ([]Instance, error)
    BillingModel() BillingModel
}
```

`Spec` carries the tier-selected instance type, optional managed image ID,
cloud-init, authorized SSH key, labels, and ownership role. The core discovers
optional behavior with small capability interfaces: `Resetter`,
`ManagedImageProvider`, `BuilderProvider`, `BuilderPromoter`, `Pricer`,
`InfoProvider`, and provider-specific dispatch selection. Adding another
ordinary cloud requires a registered provider package and its composition
import, not a new orchestrator branch.

## Worker lifecycle and reconcile

Normal reuse follows:

```text
Provisioning -> Idle -> Busy -> Idle -> Draining -> Removing
```

Strict snapshot reuse follows:

```text
Provisioning -> Idle -> Busy -> Resetting -> Idle
                                      `-----> Removing (skip/failure)
```

On each tier tick the reconcile loop:

1. Lists provider-owned workers, adopts unknown instances, drops vanished ones,
   and rebuilds teardown timing from provider `CreatedAt`.
2. Reconciles managed images/builders when snapshot reset is configured.
3. Polls Forgejo for the tier's required label and filters against its complete
   advertised label set.
4. Dispatches waiting jobs to Idle workers.
5. Provisions unmet demand plus the idle reserve, crediting in-flight future
   capacity and respecting the global tier cap.
6. Applies the provider-selected teardown policy to Idle workers.

Dispatch/teardown goroutines may update only their own locked node state. They
do not make fleet-wide provisioning decisions.

## Job dispatch and worker security

The daemon holds the Forgejo admin token. For each attempt it mints one
ephemeral registration and delivers the one-shot token over the selected
dispatcher; workers never receive the admin credential.

The cloud baseline is authenticated SSH with a per-generation ed25519 host key
injected through cloud-init and pinned before first dial. The dispatch session
also carries the reverse Forgejo port-forward and runner network configuration
needed by containerized steps. Local Docker uses an exec dispatcher addressed
by container ID. Bootstrap installs Docker, forgejo-runner, and the readiness
sentinel but no persistent job credentials.

## Deployment assumptions

SQLite is for one daemon on a persistent local filesystem; HA writers, shared
network filesystems, and database replication are outside this design. Cost
figures are provider list-price estimates, not invoice reconciliation: taxes,
discounts, credits, and network egress are not included.
