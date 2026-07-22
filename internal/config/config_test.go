package config

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hstern/fj-bellows/internal/forgejo"
)

const validLinodeConfig = `
forgejo:
  url: https://forgejo.example.com
  token: test-forgejo-token
  scope: orgs/example
database:
  path: /tmp/fj-bellows-test.db
providers:
  primary:
    driver: linode
    config:
      region: test-region
      type: test-type
      image: test-image
      token: test-provider-token
tiers:
  default:
    required_label: ubuntu-latest
    provider: primary
    instance_type: test-type
ssh:
  private_key_file: /tmp/test-id
`

func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	if name != "config.yaml" {
		t.Fatalf("writeTemp: unsupported name %q", name)
	}
	file, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(content); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return file.Name()
}

func TestLoadDefaultsAndDeferredProviderDecode(t *testing.T) {
	cfg, err := Load(writeTemp(t, "config.yaml", validLinodeConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	assertGlobalDefaults(t, cfg)
	assertTierDefaults(t, cfg.Tiers["default"])
	if got := cfg.TierNames(); !reflect.DeepEqual(got, []string{"default"}) {
		t.Fatalf("TierNames = %v", got)
	}
	if got := cfg.TierTag("default"); !strings.HasPrefix(got, tierTagPrefix) {
		t.Fatalf("TierTag = %q", got)
	}

	var providerConfig struct {
		Region string `yaml:"region"`
		Type   string `yaml:"type"`
	}
	providerDef := cfg.Providers["primary"]
	if err := providerDef.Config.Decode(&providerConfig); err != nil {
		t.Fatalf("decode provider config: %v", err)
	}
	if providerConfig.Region != "test-region" || providerConfig.Type != "test-type" {
		t.Fatalf("provider config = %+v", providerConfig)
	}
}

func TestTierTagIsStableUnambiguousAndProviderSafe(t *testing.T) {
	first := (&Config{Tag: "a-b"}).TierTag("c")
	const stable = "fjb-t-77cd13788dd21f63cb162770e9b9abef07d2d17f"
	if first != stable {
		t.Fatalf("TierTag changed: got %q, want stable %q", first, stable)
	}
	if repeated := (&Config{Tag: "a-b"}).TierTag("c"); repeated != first {
		t.Fatalf("TierTag is not stable: %q then %q", first, repeated)
	}
	if ambiguousJoin := (&Config{Tag: "a"}).TierTag("b-c"); ambiguousJoin == first {
		t.Fatalf("distinct ownership tuples collided: %q", first)
	}
	if changedTier := (&Config{Tag: "a-b"}).TierTag("d"); changedTier == first {
		t.Fatalf("different tiers produced the same tag: %q", first)
	}
	if len(first) > 50 {
		t.Fatalf("TierTag length = %d, want at most 50: %q", len(first), first)
	}
	if unsafe := strings.Trim(first, "abcdefghijklmnopqrstuvwxyz0123456789-"); unsafe != "" {
		t.Fatalf("TierTag contains provider-unsafe characters %q: %q", unsafe, first)
	}
}

func assertGlobalDefaults(t *testing.T, cfg *Config) {
	t.Helper()
	if cfg.Tag != DefaultTag || cfg.Poll.Interval.D() != 10*time.Second {
		t.Fatalf("top-level defaults: tag=%q poll=%s", cfg.Tag, cfg.Poll.Interval.D())
	}
	if cfg.SSH.User != "root" || cfg.SSH.Port != 22 {
		t.Fatalf("SSH defaults = %q:%d", cfg.SSH.User, cfg.SSH.Port)
	}
	if cfg.Database.Retention.D() != 0 {
		t.Fatalf("database retention = %s, want forever", cfg.Database.Retention.D())
	}
}

func assertTierDefaults(t *testing.T, tier Tier) {
	t.Helper()
	if !reflect.DeepEqual(tier.Labels, []string{"ubuntu-latest"}) {
		t.Fatalf("default labels = %v", tier.Labels)
	}
	if tier.ResetMode != ResetNone || tier.MaxInstances != 1 || tier.WarmInstances != 0 {
		t.Fatalf("tier scalar defaults = %#v", tier)
	}
	if tier.ResetMinRemaining.D() != 10*time.Minute || tier.ResetTimeout.D() != 5*time.Minute {
		t.Fatalf("tier reset defaults = %#v", tier)
	}
	if tier.IdleTimeout.D() != 5*time.Minute || tier.HourMargin.D() != 5*time.Minute || tier.BillingHour.D() != time.Hour {
		t.Fatalf("tier teardown defaults = %#v", tier)
	}
}

func TestLoadMultiProviderTierFieldsAndDurations(t *testing.T) {
	path := writeTemp(t, "config.yaml", `
forgejo: {url: https://forgejo.example.com, token: test-token, scope: orgs/example}
database: {path: /tmp/fj-bellows-test.db, retention: 720h}
providers:
  do-main:
    driver: digitalocean
    config: {token: test-do-token, region: test-region, image: test-image}
  hetzner-main:
    driver: hetzner
    config: {token: test-hetzner-token, location: test-location, image: test-image}
tiers:
  short:
    required_label: ci-short-amd64
    labels: [ci-short-amd64, ubuntu-latest]
    provider: do-main
    instance_type: s-4vcpu-8gb
    one_job_per_vm: true
    warm_instances: 0
    idle_timeout: 2m
    max_instances: 10
  long:
    required_label: ci-long-amd64
    provider: hetzner-main
    instance_type: cx33
    one_job_per_vm: true
    reset_mode: snapshot
    reset_min_remaining: 8m
    reset_timeout: 4m
    warm_instances: 2
    hour_margin: 3m
    billing_hour: 1h
    max_instances: 12
poll: {interval: 30s}
ssh: {private_key_file: /tmp/test-id}
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	assertMultiProviderGlobals(t, cfg)
	assertShortTier(t, cfg.Tiers["short"])
	assertLongTier(t, cfg.Tiers["long"])
	if got := cfg.TierNames(); !reflect.DeepEqual(got, []string{"long", "short"}) {
		t.Fatalf("TierNames = %v", got)
	}
}

func assertMultiProviderGlobals(t *testing.T, cfg *Config) {
	t.Helper()
	if cfg.Poll.Interval.D() != 30*time.Second || cfg.Database.Retention.D() != 720*time.Hour {
		t.Fatalf("global durations: poll=%s retention=%s", cfg.Poll.Interval.D(), cfg.Database.Retention.D())
	}
}

func assertShortTier(t *testing.T, short Tier) {
	t.Helper()
	if short.Provider != "do-main" || short.InstanceType != "s-4vcpu-8gb" || short.MaxInstances != 10 {
		t.Fatalf("short tier = %#v", short)
	}
	if !reflect.DeepEqual(short.Labels, []string{"ci-short-amd64", "ubuntu-latest"}) || short.IdleTimeout.D() != 2*time.Minute {
		t.Fatalf("short labels/timing = %#v", short)
	}
}

func assertLongTier(t *testing.T, long Tier) {
	t.Helper()
	if long.ResetMode != ResetSnapshot || !long.OneJobPerVM || long.WarmInstances != 2 || long.MaxInstances != 12 {
		t.Fatalf("long tier = %#v", long)
	}
	if long.ResetMinRemaining.D() != 8*time.Minute || long.ResetTimeout.D() != 4*time.Minute || long.HourMargin.D() != 3*time.Minute {
		t.Fatalf("long tier durations = %#v", long)
	}
}

func TestLoadRejectsLegacySchema(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{name: "provider", yaml: "provider: linode\n", want: `legacy field "provider"`},
		{name: "provider config", yaml: "provider_config: {}\n", want: `legacy field "provider_config"`},
		{name: "scale", yaml: "scale: {max: 2}\n", want: `legacy field "scale"`},
		{name: "Forgejo labels", yaml: "forgejo: {labels: [ubuntu]}\n", want: `legacy field "forgejo.labels"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, "config.yaml", tt.yaml))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{name: "top level", yaml: validLinodeConfig + "unexpected: true\n", want: "unexpected"},
		{
			name: "nested Forgejo",
			yaml: strings.Replace(validLinodeConfig, "  scope: orgs/example", "  scope: orgs/example\n  tokne: typo", 1),
			want: "tokne",
		},
		{
			name: "provider definition",
			yaml: strings.Replace(validLinodeConfig, "    driver: linode", "    driver: linode\n    drivr: typo", 1),
			want: "drivr",
		},
		{
			name: "tier",
			yaml: strings.Replace(validLinodeConfig, "    instance_type: test-type", "    instance_type: test-type\n    idle_timout: 1m", 1),
			want: "idle_timout",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, "config.yaml", tt.yaml))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load error = %v, want unknown field %q", err, tt.want)
			}
		})
	}
}

func TestLoadRequiresDatabaseProvidersAndTiers(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "database path",
			yaml: strings.Replace(validLinodeConfig, "database:\n  path: /tmp/fj-bellows-test.db\n", "", 1),
			want: "database.path",
		},
		{
			name: "providers",
			yaml: strings.Replace(validLinodeConfig, "providers:\n  primary:\n    driver: linode\n    config:\n      region: test-region\n      type: test-type\n      image: test-image\n      token: test-provider-token\n", "", 1),
			want: "providers",
		},
		{
			name: "tiers",
			yaml: strings.Replace(validLinodeConfig, "tiers:\n  default:\n    required_label: ubuntu-latest\n    provider: primary\n    instance_type: test-type\n", "", 1),
			want: "tiers",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, "config.yaml", tt.yaml))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestLoadDatabaseValidation(t *testing.T) {
	missingParent := filepath.Join(t.TempDir(), "missing", "ledger.db")
	tests := []struct {
		name        string
		replacement string
		want        string
	}{
		{name: "negative retention", replacement: "database:\n  path: /tmp/fj-bellows-test.db\n  retention: -1s", want: "retention must not be negative"},
		{name: "missing parent", replacement: "database:\n  path: " + missingParent, want: "must exist and be a directory"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			yaml := strings.Replace(validLinodeConfig, "database:\n  path: /tmp/fj-bellows-test.db", tt.replacement, 1)
			_, err := Load(writeTemp(t, "config.yaml", yaml))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestLoadDockerProviderSkipsSSHKey(t *testing.T) {
	path := writeTemp(t, "config.yaml", `
forgejo: {url: https://forgejo.example.com, token: test-token, scope: orgs/example}
database: {path: /tmp/fj-bellows-test.db}
providers:
  local:
    driver: docker
    config: {image: example/worker:latest}
tiers:
  local:
    required_label: docker
    provider: local
`)
	if _, err := Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}
}

func TestLoadNonDockerProviderRequiresSSHKey(t *testing.T) {
	yaml := strings.Replace(validLinodeConfig, "ssh:\n  private_key_file: /tmp/test-id\n", "", 1)
	_, err := Load(writeTemp(t, "config.yaml", yaml))
	if err == nil || !strings.Contains(err.Error(), "ssh.private_key_file") {
		t.Fatalf("Load error = %v, want missing SSH key", err)
	}
}

//nolint:dupl // Each table documents validation errors for a distinct configuration section.
func TestLoadTierValidation(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{name: "unknown provider", yaml: strings.Replace(validLinodeConfig, "provider: primary", "provider: missing", 1), want: "is not defined"},
		{name: "missing instance type", yaml: strings.Replace(validLinodeConfig, "    instance_type: test-type\n", "", 1), want: "instance_type is required"},
		{name: "negative max", yaml: strings.Replace(validLinodeConfig, "    instance_type: test-type", "    instance_type: test-type\n    max_instances: -1", 1), want: "max_instances must be positive"},
		{name: "warm over max", yaml: strings.Replace(validLinodeConfig, "    instance_type: test-type", "    instance_type: test-type\n    max_instances: 2\n    warm_instances: 3", 1), want: "warm_instances must be between"},
		{name: "bad reset mode", yaml: strings.Replace(validLinodeConfig, "    instance_type: test-type", "    instance_type: test-type\n    reset_mode: magic", 1), want: "reset_mode must be"},
		{name: "snapshot without one-job", yaml: strings.Replace(validLinodeConfig, "    instance_type: test-type", "    instance_type: test-type\n    reset_mode: snapshot", 1), want: "requires one_job_per_vm=true"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, "config.yaml", tt.yaml))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestLoadRejectsInvalidTimingRanges(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "negative poll interval",
			yaml: validLinodeConfig + "poll: {interval: -1s}\n",
			want: "poll.interval must be positive",
		},
		{
			name: "negative reset minimum",
			yaml: strings.Replace(validLinodeConfig, "    instance_type: test-type", "    instance_type: test-type\n    reset_min_remaining: -1s", 1),
			want: "reset_min_remaining must be positive",
		},
		{
			name: "negative reset timeout",
			yaml: strings.Replace(validLinodeConfig, "    instance_type: test-type", "    instance_type: test-type\n    reset_timeout: -1s", 1),
			want: "reset_timeout must be positive",
		},
		{
			name: "negative idle timeout",
			yaml: strings.Replace(validLinodeConfig, "    instance_type: test-type", "    instance_type: test-type\n    idle_timeout: -1s", 1),
			want: "idle_timeout must be positive",
		},
		{
			name: "negative billing hour",
			yaml: strings.Replace(validLinodeConfig, "    instance_type: test-type", "    instance_type: test-type\n    billing_hour: -1s", 1),
			want: "billing_hour must be positive",
		},
		{
			name: "negative hour margin",
			yaml: strings.Replace(validLinodeConfig, "    instance_type: test-type", "    instance_type: test-type\n    hour_margin: -1s", 1),
			want: "hour_margin must be non-negative and less than billing_hour",
		},
		{
			name: "margin reaches billing hour",
			yaml: strings.Replace(validLinodeConfig, "    instance_type: test-type", "    instance_type: test-type\n    hour_margin: 1h\n    billing_hour: 1h", 1),
			want: "hour_margin must be non-negative and less than billing_hour",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, "config.yaml", tt.yaml))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestLoadRejectsAmbiguousTierLabels(t *testing.T) {
	yaml := strings.Replace(validLinodeConfig, "    instance_type: test-type\n", `    instance_type: test-type
  second:
    required_label: second
    labels: [second, ubuntu-latest]
    provider: primary
    instance_type: test-type
`, 1)
	_, err := Load(writeTemp(t, "config.yaml", yaml))
	if err == nil || !strings.Contains(err.Error(), "advertises required label owned by tier default") {
		t.Fatalf("Load error = %v, want ambiguous label error", err)
	}
}

func TestLoadDefaultsAndInjectsAutomaticRouteLabels(t *testing.T) {
	yaml := `
forgejo: {url: https://forgejo.example.test, token: test, scope: orgs/example}
database: {path: ` + filepath.Join(t.TempDir(), "routing.db") + `}
providers:
  do: {driver: digitalocean, config: {token: test, region: test, image: test}}
  hz: {driver: hetzner, config: {token: test, location: test, image: test}}
tiers:
  short: {required_label: ci-short-amd64, provider: do, instance_type: small}
  long: {required_label: ci-long-amd64, provider: hz, instance_type: large}
routing:
  currency: usd
  exchange_rates: {eur: "1.08"}
  routes:
    amd64:
      required_label: ci-auto-amd64
      candidates: [short, long]
      fallback_tier: short
      max_optimization_wait_queue: 1
ssh: {private_key_file: /test/key}
`
	path := writeTemp(t, "config.yaml", yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Routing.Currency != "USD" || cfg.Routing.ExchangeRates["USD"] != "1" ||
		cfg.Routing.ExchangeRates["EUR"] != "1.08" {
		t.Fatalf("normalized routing currencies = %+v", cfg.Routing)
	}
	route := cfg.Routing.Routes["amd64"]
	if route.HistoryWindow.D() != 720*time.Hour || route.MinSamples != 10 ||
		route.ColdStartP95.D() != 15*time.Minute || route.MaxOptimizationWaitQueue != 1 {
		t.Fatalf("routing defaults = %+v", route)
	}
	for _, tierName := range route.Candidates {
		if !slices.Contains(forgejo.BareLabels(cfg.Tiers[tierName].Labels), route.RequiredLabel) {
			t.Fatalf("tier %s labels = %v, missing %q", tierName, cfg.Tiers[tierName].Labels, route.RequiredLabel)
		}
	}
}

//nolint:dupl // Each table documents validation errors for a distinct configuration section.
func TestLoadRejectsInvalidAutomaticRoutes(t *testing.T) {
	base := `
forgejo: {url: https://forgejo.example.test, token: test, scope: orgs/example}
database: {path: ` + filepath.Join(t.TempDir(), "routing.db") + `}
providers:
  one: {driver: digitalocean, config: {token: test}}
tiers:
  short: {required_label: ci-short, provider: one, instance_type: small}
  long: {required_label: ci-long, provider: one, instance_type: large}
routing:
  currency: USD
  exchange_rates: {USD: "1"}
  routes:
    auto:
      required_label: ci-auto
      candidates: [short, long]
      fallback_tier: short
ssh: {private_key_file: /test/key}
`
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{name: "fallback", yaml: strings.Replace(base, "fallback_tier: short", "fallback_tier: missing", 1), want: "fallback_tier"},
		{name: "candidate", yaml: strings.Replace(base, "[short, long]", "[short, missing]", 1), want: "unknown tier"},
		{name: "currency", yaml: strings.Replace(base, "currency: USD", "currency: US", 1), want: "routing.currency"},
		{name: "fx", yaml: strings.Replace(base, `USD: "1"`, `USD: "0"`, 1), want: "positive decimal"},
		{name: "samples", yaml: strings.Replace(base, "fallback_tier: short", "fallback_tier: short\n      min_samples: -1", 1), want: "must be positive"},
		{name: "optimization queue", yaml: strings.Replace(base, "fallback_tier: short", "fallback_tier: short\n      max_optimization_wait_queue: -1", 1), want: "must not be negative"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeTemp(t, "config.yaml", test.yaml)
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestLoadBadDuration(t *testing.T) {
	yaml := strings.Replace(validLinodeConfig, "    instance_type: test-type", "    instance_type: test-type\n    idle_timeout: not-a-duration", 1)
	if _, err := Load(writeTemp(t, "config.yaml", yaml)); err == nil {
		t.Fatal("expected duration parse error")
	}
}
