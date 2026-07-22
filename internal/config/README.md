# internal/config

Loads, defaults, and validates the tiers-only fj-bellows YAML schema.

`providers` is a map of named provider instances. Each entry selects a
registered `driver` and keeps its `config` subtree as an opaque `yaml.Node`, so
the core never imports cloud-specific configuration. `tiers` references those
names and defines job routing, instance type, advertised labels, strict
one-job/reset behavior, proactive idle reserve, teardown timers, and maximum
capacity.

`routing` optionally defines automatic route labels, candidate tiers, a
fallback, profiling bounds, and reproducible fixed exchange rates. Route labels
are added to every candidate tier's advertised labels. Explicit tier labels
retain precedence. Routing configuration and these derived labels are part of
the restart-only configuration fingerprint.

The required `database.path` points at the durable SQLite ledger. Its parent
must already exist; `database.retention: 0s` retains completed history forever.
Any non-Docker provider requires `ssh.private_key_file`.

Defaults include:

- deployment tag `fj-bellows` and a 10-second poll interval;
- `labels: [required_label]`;
- `one_job_per_vm: false`, `reset_mode: none`, and `warm_instances: 0`;
- `max_instances: 1`;
- five-minute idle timeout/hour margin/reset timeout;
- ten-minute minimum reset runway and a one-hour billing cycle;
- automatic-route history window 720 hours, minimum 10 samples, and cold P95
  15 minutes; and
- SSH user `root`, port 22.

Validation prevents undefined providers, ambiguous required-label ownership,
warm capacity above the tier maximum, and snapshot reset without strict
one-job mode. It also requires at least two distinct known candidates per
automatic route, a candidate fallback, unique route labels, and positive
fixed-point exchange rates. Unknown typed configuration fields are rejected,
while each provider's opaque `config` subtree is validated strictly by that provider. Tier
ownership tags are stable hashes of a length-delimited `(deployment tag, tier)`
tuple, avoiding ambiguous concatenation and provider length limits. The removed
`provider`, `provider_config`, `scale`, and `forgejo.labels` fields produce an
explicit migration error.

`Redact(*Config)` returns a control-plane-safe deep copy. It replaces the
Forgejo token and recursively redacts credential-shaped scalar keys in every
named provider's opaque config while preserving driver names, tier data, and
the SSH key path.
