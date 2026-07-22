# AGENTS.md

Repository guidance for humans and coding agents working on fj-bellows.

## Project overview

fj-bellows is a pluggable autoscaler for ephemeral Forgejo Actions runners. A
single daemon manages several independently scaled tiers, provisions workers
through registered providers, routes automatic-label jobs by predicted
marginal cost, and records recovery/accounting state in SQLite.

This fork extends the original single-provider/single-pool Linode baseline
(`origin/main` at `9d0b51c` when the fork work began) with:

- named providers and tiers;
- DigitalOcean and Hetzner providers;
- Hetzner golden-snapshot rebuilds and warm paid-hour reuse;
- durable P95/cost-aware automatic routing and a bounded optimization queue;
- SQLite recovery, job statistics, pricing, costs, and routing effectiveness;
- an embedded live dashboard with JSON and WebSocket endpoints; and
- expanded control-plane reporting and `fjbctl` commands.

Read [README.md](README.md) for operator-facing behavior and
[docs/design.md](docs/design.md) for the architecture.

## Build and verification

The mandatory test path has no tool dependency beyond Go and the committed
module graph:

```sh
make build              # go build ./...
make test               # go test ./...
make race               # go test -race ./... (required for concurrent code)
go vet ./...
```

`govulncheck` is intentionally not a Make target, Docker build step, or CI
gate. It requires a network-backed external database and must not prevent the
hermetic test suite from completing. Downstream release pipelines may run a
scanner of their choice.

Optional developer tools are separate from the test path:

```sh
make lint               # golangci-lint run ./...
make lint-fix
make fmt
make proto              # regenerate committed protobuf/ConnectRPC code
make proto-check        # regenerate and fail on gen/ drift
```

The proto targets require `buf`, `protoc-gen-go`, and
`protoc-gen-connect-go`. Only install/run them when changing `proto/`; commit
the regenerated `gen/` tree. `//nolint` directives must name the linter and
include a real reason.

## Repository map

| Path | Responsibility |
| --- | --- |
| `cmd/fj-bellows` | composition root, recovery, singleton lock, provider/tier wiring |
| `cmd/fjbctl` | control-plane CLI, job/statistics/routing reporting |
| `internal/config` | strict tiers-only YAML parsing, defaults, validation, redaction |
| `internal/forgejo` | Actions queue polling and ephemeral registration |
| `internal/provider` | provider contracts, optional capabilities, registry |
| `internal/provider/digitalocean` | per-second DigitalOcean droplets and pricing |
| `internal/provider/hetzner` | hourly servers, snapshots, rebuild reset, pricing |
| `internal/provider/linode` | hourly Linodes and managed network/cache resources |
| `internal/provider/docker` | local CLI-backed containers and exec dispatch |
| `internal/orchestrator` | per-tier reconcile, worker state, reset, teardown, accounting |
| `internal/router` | durable automatic job-to-tier decisions and paid-window queue |
| `internal/storage` | SQLite migrations, recovery, history, costs, statistics |
| `internal/control` | ConnectRPC, health, metrics, dashboard API/UI/WebSocket |
| `internal/bootstrap` | worker/builder cloud-init and snapshot scrub contract |

Every package must have a `README.md`. Add/update that local documentation with
new package behavior rather than relying only on top-level docs.

## Configuration contract

The supported schema is tiers-only:

```yaml
database:
  path: /var/lib/fj-bellows/fj-bellows.db

providers:
  cloud:
    driver: hetzner
    config: {token: <token>, location: fsn1, image: debian-13}

tiers:
  long:
    required_label: ci-long-amd64
    provider: cloud
    instance_type: cx33
    one_job_per_vm: true
    reset_mode: snapshot
    max_instances: 10
```

Legacy top-level `provider`, `provider_config`, `scale`, and
`forgejo.labels` fields are rejected. Provider `config` remains an opaque
`yaml.Node` until the selected driver strictly decodes it. Do not add
provider-specific fields to core config or orchestration packages.

`config.yaml` contains Forgejo/provider tokens and must remain mode `0600`.
The SSH private key is referenced by path. The SQLite parent directory must
exist on a persistent local filesystem; shared NFS and multiple daemon writers
are unsupported. Never commit real tokens, hosts, account IDs, or deployment
regions from a private environment.

## Architecture invariants

### Fleet and providers

- A tier owns one required label, provider reference, instance type, capacity,
  warm/reset policy, and reconcile loop. Do not reintroduce global scale or
  provider settings.
- Billing behavior is a provider attribute. `BillingPerSecond` uses idle
  timeout; `BillingHourlyRoundUp` retains useful capacity until the configured
  margin before the next provider-anchored paid boundary.
- `Instance.CreatedAt` comes from the provider and is the immutable billing
  anchor. Derive teardown windows each tick so they survive restarts and
  in-place Hetzner rebuilds.
- Provider ownership is based only on the derived deployment+tier tag. `List`
  is cloud truth for workers; deployments sharing an account need distinct
  top-level tags.
- Optional behavior stays behind small interfaces (`Pricer`, `Resetter`,
  `ManagedImageProvider`, `BuilderProvider`, `BuilderPromoter`,
  `InfoProvider`). Do not add provider-name branches to the core.

### Reconcile and durable recovery

- A tier reconcile loop is the single writer of fleet-wide provisioning
  decisions. Dispatch/reset/teardown goroutines may mutate only their locked
  node state. Provisioning, resetting, and image building all consume future
  capacity and must not exceed `max_instances`.
- Persist mutation intent before external provider side effects. Provider APIs
  answer whether a resource exists; SQLite answers what the daemon intended
  and preserves history/accounting.
- Startup recovery reconciles tagged provider resources with open resources,
  generations, phases, snapshots, and mutations. Recovery must be idempotent;
  never double-charge or redispatch a claimed job.
- Retention removes completed history only. It must preserve open resources,
  jobs, phases, snapshots, mutations, and live routing assignments.

### Cost-aware routing and queueing

- Automatic routing is fleet-level and per job, not a provider meta-driver.
  Explicit tier labels always take precedence over an automatic label.
- Persist a routing decision and immutable candidate scorecards before the
  selected tier may see the job. Assignments and queue payloads must replay
  after restart.
- Profiles are isolated by Forgejo source, repository, workflow, and job name.
  Include normal successes/failures; exclude cancellations, skips,
  infrastructure failures, and interrupted attempts.
- Score provider quotes in integer nanounits with configured fixed FX rates.
  Unknown/stale pricing degrades routing visibility but must never silently
  become zero cost.
- `max_optimization_wait_queue` limits intentional waiting behind already-paid
  hourly workers. A held job must fit, including reset P95, before that exact
  worker's reap boundary. Only exact held handles suppress provisioning; they
  must never be removed from dispatch visibility.
- Reconstruct paid-window lanes from SQLite on restart. A vanished worker or
  expired window releases the optimization hold while preserving the durable
  tier assignment and historical scorecard.

### Hetzner snapshot reset

- `one_job_per_vm: true` plus `reset_mode: snapshot` means rebuild the same
  server allocation from an owned clean image. Preserve server ID,
  provider `CreatedAt`, and therefore the paid-hour anchor.
- Builders, workers, and images have distinct ownership roles. Builders must
  never appear in normal worker `List`; use the builder recovery capability.
- The golden image fingerprint covers tier/provider configuration, versions,
  bootstrap, and the snapshot scrub contract. Never accept an unrelated image
  as a managed reset image.
- Sysprep removes cloud-init state, machine identity, SSH material, logs, and
  runner/job state before snapshot capture. The image must not contain Forgejo
  registration/admin tokens, provider credentials, or deployment keys.
- Promote the first successful builder to a worker and rebuild that same paid
  allocation. Delete it only when handoff/reset fails or insufficient paid time
  remains.

### Dispatch and worker security

- Worker VMs never receive a Forgejo admin token. Mint an ephemeral one-shot
  registration per attempt and deliver it over the dispatcher, never argv.
- Dispatch stays mechanism-independent. SSH uses the address; Docker exec uses
  the provider ID. Cloud SSH host keys are generated per filesystem generation
  and pinned before the first connection; never disable host-key verification.
- The SSH session carries Forgejo reverse forwarding. Preserve the worker
  `/etc/hosts` override and runner `container.network: host`,
  `container.options: --add-host=...`, and
  `container.docker_host: automount` settings so containerized steps reach
  Forgejo and Docker correctly.

### Dashboard and events

- `/dashboard/` is a read-only embedded shell. `/dashboard/api` is the
  authoritative snapshot; `/dashboard/ws` carries bounded fleet events that
  trigger a debounced refresh. Five-second polling remains the loss/reconnect
  fallback.
- With control auth enabled, static HTML/CSS/JS may remain public because it
  contains no fleet data. Protect API and WebSocket data. Browser WebSockets
  authenticate through `fjb-bearer.<base64url token>` and select
  `fjb-events-v1`; never put credentials in a query string.
- Event subscribers must not block reconcile. The existing bus is bounded and
  may drop intermediate notifications because the next API snapshot is the
  source of truth.

### Linode managed resources

- The managed cache VM and its bucket/key outlive the worker fleet. Do not reap
  them from per-worker `Destroy`; `cache.ensure()` recreates unrelated loss.
- Managed firewall, placement group, and VPC resources are lazy-recreatable
  after last-worker reaping. Any new managed resource needs an `ensure()` path
  before its ID is read during `Provision`.
- The managed cache is an explicit scratch registry, not a transparent
  pull-through proxy. Do not add registry traffic interception without solving
  Docker/OCI manifest compatibility at the same time.
- Runtime firewall auto-refresh failures retain the previous known-good rules;
  initial `auto` resolution failure is fatal.

## Testing and change discipline

- Add fast, hermetic unit tests for every behavior. Cloud/provider tests use
  hand-written concurrency-safe mocks; `go test` must not require cloud
  credentials, Docker, or internet access.
- Always run `go test -race ./...` after orchestrator, router, event, pool, or
  storage concurrency changes.
- Use forward-only embedded SQLite migrations. Never edit an already released
  migration; add a new version and cover migration/retention/replay.
- Generated protobuf files are committed. Run `make proto` after proto edits
  and verify `make proto-check` before submitting.
- Preserve unrelated user changes in a dirty worktree. Use `gofmt` and keep
  `git diff --check` clean.

## Adding another provider

1. Add `internal/provider/<name>` implementing `provider.Provider`.
2. Strictly decode only its opaque YAML with `provider.DecodeConfig`.
3. Register the driver in `init` and blank-import it from
   `cmd/fj-bellows/main.go`.
4. Return the correct billing model and provider-owned `CreatedAt`.
5. Implement `Pricer` if the tier should participate in automatic routes; add
   optional reset/image interfaces only when the cloud supports them safely.
6. Add a package `README.md`, hand-written client mocks, and hermetic tests for
   ownership, billing, errors, and price conversion.

## Deliberate limitations

- Workers are capacity one; concurrent jobs inside one VM are not supported.
- Routing does no intentional exploration; profiles grow from fallback and
  subsequent routed executions.
- SQLite supports one daemon on local persistent storage, not HA writers.
- Cost reports are list-price estimates. Taxes, discounts, credits, and egress
  are outside direct per-job marginal scoring.
