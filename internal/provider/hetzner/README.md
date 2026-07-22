# Hetzner provider

The `hetzner` provider creates ephemeral Hetzner Cloud servers through the
official `hcloud-go/v2` SDK. It advertises hourly-round-up billing, so the core
keeps idle workers useful until the configured margin before their next paid
hour boundary.

```yaml
providers:
  hetzner:
    driver: hetzner
    config:
      token: <hetzner-token>
      locations: [fsn1, nue1, hel1]
      image: debian-13
      # network_id: 123
      # firewall_ids: [456]
      # pricing_override:
      #   currency: EUR
      #   instances:
      #     cx33:
      #       per_hour: "0.0123"
      #       per_month: "7.49"
      #   snapshot_gb_month: "0.0119"
```

The tier supplies `instance_type`; the provider configuration supplies an
ordered location list and base image. Provisioning tries locations in order and
uses the first successful one. Only capacity, placement, maintenance, and
location-network availability errors advance to the next location; permanent
errors such as a bad token fail immediately. The legacy scalar `location` is
still accepted, but it is mutually exclusive with `locations`. If `network_id`
is set, that Network must be usable from every fallback location.

Servers always receive public IPv4 and may also attach to one existing Network
and existing Firewalls. The orchestrator SSH key is installed through
multipart cloud-init, which also works during a server rebuild (the rebuild API
has no SSH-key parameter).

## Ownership and snapshots

Resources carry provider-safe labels derived from the deployment tag. Worker,
builder, and image roles are distinct: normal `List(tag)` only returns workers,
so snapshot builders cannot be adopted into the dispatch pool. The separate
`ListBuilders(tag)` recovery capability lets the core reap a builder left by a
daemon crash.

Managed snapshots are created only from an owned builder. The SSH dispatcher
synchronously scrubs credentials and machine identity and calls `sync` before
returning. `CreateImage` then requests shutdown (with hard-poweroff fallback),
verifies the server is stopped, and only then captures the root disk. Once the
snapshot is durable, `PromoteBuilder` changes only the verified ownership role
to worker and the core rebuilds the same server from that snapshot. This lets
the first job reuse the builder's already-paid allocation rather than opening a
second hourly minimum.
The fingerprint is split across labels without truncation. New workers and
resets accept only snapshots carrying matching ownership labels; arbitrary
operator image IDs are not accepted as managed reset images.

`Reset` rebuilds the existing server allocation and deliberately returns its
original provider `CreatedAt`, preserving the billing-hour anchor used by the
core teardown policy. Hetzner snapshots contain the root disk only; attached
Volumes are outside this provider's managed snapshot lifecycle.

## Diagnostics and pricing

`Info` exposes the ordered locations, first location (for backward-compatible
diagnostics), base image, attachment IDs, and tag, never the token. `Quote`
reads Hetzner's location-specific net catalog prices and returns fixed-point
nanounits for compute and snapshot storage. With multiple locations it requires
a catalog entry for each one and conservatively uses the highest hourly and
monthly rates, since the eventual fallback location is unknown while routing.
Optional decimal-string overrides take precedence. Currency mismatches are
rejected when a partial override would otherwise mix amounts from two
currencies.

The narrow `Client` interface isolates `hcloud-go` from provider behavior. Its
sibling `mock` package is hand-written and concurrency-safe; all tests are
hermetic and perform no cloud or network calls.
