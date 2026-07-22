# fj-bellows

A pluggable, ephemeral CI-runner autoscaler for [Forgejo Actions](https://forgejo.org/).

fj-bellows polls a Forgejo instance's Actions queue and routes each job to an
independently scaled **tier**. Tiers select a named provider instance, machine
type, labels, capacity limit, and warm/reset policy. DigitalOcean, Hetzner,
Linode, and local Docker all plug into the same provider registry.

Workers run Forgejo's ephemeral `one-job` mode. Depending on the tier, a worker
is destroyed after that attempt, reused until its paid-hour boundary, or reset
to a clean Hetzner golden snapshot before entering the warm pool again. A
durable SQLite ledger records recovery intent, lifecycle timings, workflow/job
history, immutable price quotes, estimated costs, and reproducible automatic
routing decisions.

## What this fork changes

This branch is a substantial extension of the original single-pool Linode
autoscaler baseline (`origin/main` at `9d0b51c`, when this work started). The
important differences are:

| Area | Original baseline | This fork |
| --- | --- | --- |
| Fleet model | one provider and one global pool | named provider instances and independently scaled tiers |
| Providers | Linode, plus local Docker work in progress | Linode, DigitalOcean, Hetzner, and local Docker behind the same registry |
| Hetzner reuse | no Hetzner implementation | hourly warm reuse and clean in-place rebuilds from a managed golden snapshot |
| Job routing | one explicit label set | explicit tier labels plus durable P95/marginal-cost automatic routes |
| Burst behavior | provision immediately after idle capacity is consumed | optional bounded wait queue behind an already-paid hourly worker |
| Persistence | provider-list crash recovery only | SQLite intent, lifecycle, jobs, prices, costs, routing decisions, and restart replay |
| Reporting | live control-plane state | workflow/tier/provider statistics, costs, routing effectiveness, and queue visibility |
| Web UI | none | embedded dashboard with authenticated JSON and instant WebSocket event refresh |

The configuration format is intentionally incompatible with the original
top-level `provider`, `provider_config`, `scale`, and `forgejo.labels` fields.
Migrate deployments to `providers`, `tiers`, and a persistent `database.path`
before replacing an original binary. See [config.example.yaml](config.example.yaml).

The mandatory build/test path uses the Go toolchain only (`go vet`, build, and
race tests). Network-backed vulnerability scanners are not a repository gate;
operators may run their preferred scanner in their own release pipeline.

## Why

Some clouds (including Hetzner) bill whole hours of instance existence
**rounded up**: once a job spins a VM, the entire hour is paid regardless. So:

- Keep the VM **warm** for that paid hour — jobs 2..N start instantly instead of
  paying a fresh ~30–60s boot each time.
- **Idle-kill near the hour boundary** (at `creation + N*hour - margin`; the
  default 5-minute margin gives the `:55` rule, so the DELETE finishes before
  the next hour bills).
- A **busy** job rolls into the next paid hour; you never pay an idle hour.

For **per-second** billing clouds, warm-holding is pointless, so fj-bellows uses
a plain idle timeout instead. The billing model is a **provider attribute**, not
a hardcoded assumption — that is what makes the tool correct across clouds, not
just cheap on one.

`warm_instances` is proactive capacity, not the billing policy: zero means
"do not pre-provision." An hourly worker created on demand may still remain
useful until its already-paid boundary. For strict isolation,
`one_job_per_vm: true` either destroys the worker (`reset_mode: none`) or, on
Hetzner, rebuilds the same server from a managed clean snapshot
(`reset_mode: snapshot`). The latter preserves the server's original billing
anchor and IP while giving the next job a fresh root filesystem.

## How it works

- **Poll and route**: explicit tier labels go directly to that tier. Optional
  automatic labels are polled once by a fleet-level router, which evaluates
  compatible healthy capacity, persists one assignment, and only then exposes
  the job to the selected tier. Configuration rejects ambiguous label
  ownership, so one queued job cannot be claimed by two tiers.
- **Ephemeral per-job**: the orchestrator registers a one-shot runner
  (`POST .../actions/runners {"ephemeral":true}`) and runs `forgejo-runner
  one-job ... --wait` on the warm VM over SSH. Forgejo invalidates the
  credentials and removes the registration after the single job. The worker VM
  never holds an admin token.
- **Workers reach Forgejo over the dispatch SSH session.** The orchestrator
  opens a reverse port-forward on the same SSH connection and injects a
  `/etc/hosts` override on the worker, so a LAN-internal Forgejo whose hostname
  does not resolve from the public internet works out of the box. No
  worker-side DNS, proxy, or VPN configuration required; TLS validation against
  the original hostname is unchanged.
- **Reconcile**: each tier has one single-writer reconcile loop. It converges
  waiting jobs, runners, provider resources, configured idle reserve, and the
  tier's capacity limit. Provisioning/resetting workers count toward future
  capacity; snapshot builders count toward `max_instances`.
- **Clean reset**: a warm Hetzner snapshot tier builds a scrubbed golden image
  eagerly; a scale-to-zero tier starts that builder on its first demand. The
  builder is rebuilt as the first worker, reusing the allocation's already-paid
  hour instead of creating a second VM. After every job attempt the same
  reset/host-key/readiness cycle returns it to Idle. A reset too close to the
  next billable hour is skipped and the server is deleted instead.
- **Orphan sweep**: every instance is tagged; instances unknown to the
  orchestrator or idle past their paid hour are destroyed, so a crash or a
  failed DELETE never leaks a billed VM.
- **Singleton lock**: an advisory file lock ensures only one daemon makes
  provisioning decisions.
- **Durable ledger**: provider mutations are recorded before external side
  effects. On restart, SQLite intent is reconciled with provider-tagged ground
  truth. The same database retains workflow identity, outcomes, queue/run/reset
  timings, fixed-point cost estimates, and known-price coverage.

## Requirements

- Forgejo **≥ v15.0** (job-queue API) and **forgejo-runner > 12.5** (ephemeral
  `one-job`).
- A Forgejo admin token (to mint registration tokens).
- A supported cloud provider account and API token (or local Docker).
- An SSH keypair the orchestrator uses to dispatch jobs to worker VMs.
- A writable persistent local directory for the SQLite database. The ledger is
  created with mode `0600` and insecure existing files are rejected. Shared
  NFS and multiple daemon writers are unsupported.

## Configuration

See [`config.example.yaml`](config.example.yaml). The schema is tiers-only:

```yaml
database:
  path: /var/lib/fj-bellows/fj-bellows.db

providers:
  cloud-main:
    driver: hetzner
    config: {token: <provider-token>, location: fsn1, image: debian-13}

tiers:
  long:
    required_label: ci-long-amd64
    provider: cloud-main
    instance_type: cx33
    one_job_per_vm: true
    reset_mode: snapshot
    warm_instances: 0
    max_instances: 10

routing:
  currency: USD
  exchange_rates: {USD: "1", EUR: "1.08"}
  routes:
    amd64:
      required_label: ci-auto-amd64
      candidates: [short, long]
      fallback_tier: short
      history_window: 720h
      min_samples: 10
      cold_start_p95: 15m
      max_optimization_wait_queue: 1
```

With `warm_instances: 0`, the first matching job creates the snapshot and the
same allocation serves that job, then stays reusable only through its current
paid window. Set it to a positive number only when an always-ready proactive
reserve is worth carrying across billing boundaries.

The automatic route uses the fallback tier until it has ten qualifying
workflow-job samples. It then scores candidate marginal cost from the observed
P95 runtime, provisioning/reset P95, live free workers, provider billing
rounding/minimums/monthly caps, and fixed configured exchange rates. An idle
hourly worker inside its paid window can therefore score zero. Explicit labels
such as `ci-long-amd64` always take precedence over `ci-auto-amd64`. Routing
configuration and derived labels require a restart.

`max_optimization_wait_queue` bounds how many jobs this route may intentionally
hold behind already-paid hourly workers. A held job must fit, using job and
snapshot-reset P95, before the worker's reap boundary. Once the bound is full,
additional jobs score normal idle or cold capacity and can trigger a new VM.
The default is `0`, preserving immediate provisioning.

Each named provider's `config` subtree is opaque to the core and decoded by its
driver. `labels` defaults to `[required_label]`. The legacy top-level
`provider`, `provider_config`, `scale`, and `forgejo.labels` fields are rejected
with a migration error.

## Build & run

```sh
go build ./cmd/fj-bellows
./fj-bellows -config config.yaml
```

All secrets (Forgejo + provider tokens) live inline in `config.yaml`; the SSH
key is referenced by path. See [`config.example.yaml`](config.example.yaml).

### Web dashboard

The control listener serves a read-only live dashboard at
`http://127.0.0.1:9876/dashboard/` (and redirects `/` there). It refreshes
every five seconds and combines live VM state, billing/reap windows, active
tasks, the durable waiting queue—including cost-optimization holds—recent
outcomes, costs, P95 timings, and automatic-routing effectiveness.
Worker/job/router events arrive over `/dashboard/ws` and trigger an immediate
snapshot refresh; five-second polling remains as a disconnect-safe fallback.

The listener is loopback-only by default. On a non-loopback bind,
`-control-token-file` remains mandatory. Static dashboard assets contain no
fleet data and stay accessible so the page can load; its `/dashboard/api`
endpoint uses the same bearer token as ConnectRPC and keeps a token entered in
the UI in browser `sessionStorage` only. The WebSocket authenticates with a
base64url bearer subprotocol instead of placing credentials in its URL.

### Container image

Multi-arch images (`linux/amd64`, `linux/arm64`) are published by CI to
`ghcr.io/hstern/fj-bellows` (and to Docker Hub when configured). Run with your
`config.yaml` mounted (and the SSH key it references):

```sh
docker run -d --name fj-bellows \
  -v /etc/fj-bellows:/etc/fj-bellows:ro \
  -v /var/lib/fj-bellows:/var/lib/fj-bellows \
  ghcr.io/hstern/fj-bellows:latest
```

The image runs as the distroless `nonroot` user (uid 65532) and ships with
`/run/fj-bellows.lock` pre-created and owned `65532:65532` so the singleton
lock works out of the box — no extra mounts needed.

If you overlay a fresh tmpfs on `/run` (some systemd unit / Quadlet styles
do), the pre-created lock file is shadowed and the daemon falls back to
creating it itself, which needs a writable `/run`. Two compatible options:

1. **Leave `/run` alone** (default; recommended). The pre-created lock
   suffices.
2. **`tmpfs /run` with sticky-bit-writable mode** so the daemon can create
   the lock fresh:

   ```ini
   [Container]
   Image=ghcr.io/hstern/fj-bellows:latest
   Volume=/host/fj-bellows:/etc/fj-bellows:ro
   Tmpfs=/run:size=64k,mode=1777
   ```

Available tags:

| Tag | Points to |
|-----|-----------|
| `:latest` | latest main HEAD (bleeding edge) |
| `:0.1.0` | a specific release |
| `:0.1` | latest 0.1.x release |
| `:0` | latest 0.x release |
| `:<sha>` | an immutable commit |

The floating tags (`:0.1`, `:0`, `:latest`) are only moved forward, never
backward — a backport release after a higher version has shipped publishes
only its exact version + `:<sha>`.

## Repository layout

| Path | Purpose |
|------|---------|
| `cmd/fj-bellows` | daemon entrypoint, wiring, singleton lock |
| `internal/config` | tiers-only YAML config with deferred named-provider decode |
| `internal/forgejo` | Forgejo Actions REST client |
| `internal/provider` | provider contracts, optional capabilities, and registry |
| `internal/provider/digitalocean` | per-second DigitalOcean Droplets |
| `internal/provider/hetzner` | Hetzner servers, golden snapshots, and rebuilds |
| `internal/provider/linode` | Linode implementation |
| `internal/bootstrap` | cloud-init worker bootstrap template |
| `internal/orchestrator` | tier fleet, state machines, reconcile, reset, dispatch |
| `internal/router` | durable cost-aware automatic job-to-tier routing |
| `internal/storage` | durable SQLite recovery and accounting ledger |

## Running multiple deployments

fj-bellows decides which cloud resources it owns **solely by tags** derived
from the deployment `tag` and tier name. It only lists, adopts, resets, or
destroys matching resources. Deployments sharing a cloud account therefore
need distinct top-level tags; otherwise equal tier names produce equal
ownership tags and the daemons can act on each other's resources. The
singleton lock coordinates one host only. The daemon warns when the default
tag is used.

## Security notes

- **config.yaml holds secrets** (Forgejo + provider tokens). Keep it `chmod 600`;
  the daemon warns on startup if it (or the SSH key) is readable by other users.
- **SSH host keys are verified.** For each worker the orchestrator generates a
  fresh ed25519 host key, injects the private half via cloud-init, and pre-pins
  the public half — so the SSH dispatch connection is verified on the very first
  dial, with no trust-on-first-use window for a man-in-the-middle to capture the
  one-shot token. (If no host key is seeded, the dispatcher falls back to
  per-VM trust-on-first-use pinning.)
- The worker VM never holds the Forgejo admin token; only a single-use ephemeral
  registration token, delivered over SSH via stdin (never the command line).
- CI runs `go vet`, repository-wide race tests, and a separate lint job.

## Providers and isolation modes

| Driver | Billing policy | Clean one-job behavior |
| --- | --- | --- |
| `digitalocean` | per second, provider minimum and monthly cap recorded | destroy and refill |
| `hetzner` | hourly round-up, reap near paid boundary | destroy, or managed snapshot rebuild |
| `linode` | hourly round-up | destroy and refill when strict one-job is enabled |
| `docker` | per second/local | remove and recreate the container |

The provider declares billing behavior; the orchestrator owns teardown policy.
Snapshot reset is an optional provider capability and is currently implemented
by Hetzner only.

## License

MIT — see [LICENSE](LICENSE).
