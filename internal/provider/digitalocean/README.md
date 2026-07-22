# DigitalOcean provider

This package provisions ephemeral DigitalOcean Droplets through the official
[`godo`](https://github.com/digitalocean/godo) SDK. It implements the common
provider interface without exposing DigitalOcean-specific configuration to the
orchestrator.

## Configuration

```yaml
providers:
  digitalocean:
    driver: digitalocean
    config:
      token: <digitalocean-token>
      region: <region-slug>
      image: <image-slug-or-id>
      # vpc_uuid: <existing-vpc-uuid>
      # ssh_key_ids: [12345]
      # firewall_id: <existing-firewall-uuid>
      # pricing_override:
      #   currency: USD
      #   minimum_charge: "0.01"
      #   snapshot_gb_month: "0.06"
      #   instances:
      #     s-4vcpu-8gb:
      #       per_hour: "0.07143"
      #       per_month: "48"
```

`token`, `region`, and `image` are required. A tier supplies the Droplet size
through `instance_type`; a managed image ID in the provision spec overrides
the configured base image. Decimal prices are strings with at most nine
fractional digits so persisted accounting never depends on binary floating
point. The USD minimum charge defaults to `0.01`; another currency requires an
explicit `minimum_charge` and complete hourly/monthly rates for every quoted
size, preventing foreign-currency values from being mixed with the USD catalog.

The provider explicitly enables public IPv4 networking. `vpc_uuid`,
`ssh_key_ids`, and `firewall_id` refer only to existing operator-managed
resources. DigitalOcean attaches a firewall after Droplet creation; if that
step fails, the provider attempts to delete the new Droplet before returning
the error. The provider adds the orchestrator-provided authorized key to the
cloud-init payload with an idempotent `authorized_keys` installer;
`ssh_key_ids` are an optional additional DigitalOcean account-level mechanism,
not a prerequisite for SSH dispatch.

## Lifecycle and accounting

Every Droplet receives the provision spec's ownership tag. `List` uses the
DigitalOcean tag filter and exhausts every API page, so restart reconciliation
and orphan cleanup see the complete deployment-owned fleet. `Destroy` deletes
only the requested numeric Droplet ID. Provisioning polls the Droplet endpoint
for a bounded period and returns only after the Droplet is active with a public
IPv4. A readiness failure triggers the same best-effort cleanup used for
firewall attachment and response-validation failures.

DigitalOcean reports `BillingPerSecond`. Catalog quotes come from the Sizes API
and contain the hourly rate, monthly cap, one-second quantum, 60-second minimum
duration, and USD 0.01 minimum charge. Per-size configuration overrides can
replace either or both catalog rates; a complete override avoids the catalog
request. Snapshot storage is only populated when configured explicitly.

`Info` is network-free and returns these stable keys:

- `region`
- `image`
- `vpc_uuid`
- `firewall_id`
- `ssh_key_count`
- `tag`

It never returns the API token.

## Tests

The sibling `mock` package is a hand-written, concurrency-safe fake of the
narrow client interface. Provider tests use it exclusively and make no real
DigitalOcean or network calls.
