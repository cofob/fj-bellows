// Package config loads and validates fj-bellows YAML configuration.
//
// Provider configuration is deliberately opaque to the core. A deployment
// declares named provider instances and tiers refer to those names; each
// provider decodes its own Config yaml.Node.
package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/hstern/fj-bellows/internal/forgejo"
)

// Config is the top-level, tiers-only configuration schema.
type Config struct {
	Forgejo   Forgejo                     `yaml:"forgejo"`
	Database  Database                    `yaml:"database"`
	Providers map[string]ProviderInstance `yaml:"providers"`
	Tiers     map[string]Tier             `yaml:"tiers"`
	Routing   Routing                     `yaml:"routing"`
	Poll      Poll                        `yaml:"poll"`
	SSH       SSH                         `yaml:"ssh"`
	Transport Transport                   `yaml:"transport"`
	Tag       string                      `yaml:"tag"`
}

// Forgejo describes how to reach one Actions API scope.
type Forgejo struct {
	URL   string `yaml:"url"`
	Token string `yaml:"token"`
	Scope string `yaml:"scope"`
}

// Database configures the durable SQLite ledger. Retention zero keeps
// completed history forever.
type Database struct {
	Path      string   `yaml:"path"`
	Retention Duration `yaml:"retention"`
}

// ProviderInstance selects a registered driver and leaves the config subtree
// for that driver to decode.
type ProviderInstance struct {
	Driver string    `yaml:"driver"`
	Config yaml.Node `yaml:"config"`
}

// Tier is one independently scaled runner pool.
type Tier struct {
	RequiredLabel     string   `yaml:"required_label"`
	Labels            []string `yaml:"labels"`
	Provider          string   `yaml:"provider"`
	InstanceType      string   `yaml:"instance_type"`
	OneJobPerVM       bool     `yaml:"one_job_per_vm"`
	ResetMode         string   `yaml:"reset_mode"`
	ResetMinRemaining Duration `yaml:"reset_min_remaining"`
	ResetTimeout      Duration `yaml:"reset_timeout"`
	WarmInstances     int      `yaml:"warm_instances"`
	IdleTimeout       Duration `yaml:"idle_timeout"`
	HourMargin        Duration `yaml:"hour_margin"`
	BillingHour       Duration `yaml:"billing_hour"`
	MaxInstances      int      `yaml:"max_instances"`
}

// Routing configures cost-aware dispatch of jobs carrying an automatic label.
// Routes target ordinary tiers; provider lifecycle and billing behavior stay
// owned by those tiers.
type Routing struct {
	Currency      string            `yaml:"currency"`
	ExchangeRates map[string]string `yaml:"exchange_rates"`
	Routes        map[string]Route  `yaml:"routes"`
}

// Route maps one Forgejo label to a set of candidate tiers.
type Route struct {
	RequiredLabel            string   `yaml:"required_label"`
	Candidates               []string `yaml:"candidates"`
	FallbackTier             string   `yaml:"fallback_tier"`
	HistoryWindow            Duration `yaml:"history_window"`
	MinSamples               int      `yaml:"min_samples"`
	ColdStartP95             Duration `yaml:"cold_start_p95"`
	MaxOptimizationWaitQueue int      `yaml:"max_optimization_wait_queue"`
}

const (
	// ResetNone destroys strict one-job workers after the attempt.
	ResetNone = "none"
	// ResetSnapshot rebuilds strict one-job workers from a managed image.
	ResetSnapshot = "snapshot"
)

// Poll contains fleet-wide reconciliation settings. Billing and idle timers
// live on tiers because providers and machine classes may differ.
type Poll struct {
	Interval Duration `yaml:"interval"`
}

// SSH configures the shared SSH dispatcher used by cloud providers.
type SSH struct {
	User           string `yaml:"user"`
	PrivateKeyFile string `yaml:"private_key_file"`
	Port           int    `yaml:"port"`
}

// DefaultTag is the ownership prefix used when none is configured.
const DefaultTag = "fj-bellows"

// ProviderDocker names the local provider, which needs no SSH key.
const ProviderDocker = "docker"

// Load reads, parses, defaults, and validates a tiers-only config file.
func Load(path string) (*Config, error) {
	//nolint:gosec // operator-supplied config path.
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	if key := legacyTopLevelKey(b); key != "" {
		return nil, fmt.Errorf("config: legacy field %q is no longer supported; migrate to named providers and tiers", key)
	}
	var c Config
	decoder := yaml.NewDecoder(bytes.NewReader(b))
	decoder.KnownFields(true)
	if err := decoder.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func legacyTopLevelKey(b []byte) string {
	var doc yaml.Node
	if yaml.Unmarshal(b, &doc) != nil || len(doc.Content) == 0 {
		return ""
	}
	n := doc.Content[0]
	if n.Kind != yaml.MappingNode {
		return ""
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		switch n.Content[i].Value {
		case "provider", "provider_config", "scale":
			return n.Content[i].Value
		case "forgejo":
			v := n.Content[i+1]
			for j := 0; v.Kind == yaml.MappingNode && j+1 < len(v.Content); j += 2 {
				if v.Content[j].Value == "labels" {
					return "forgejo.labels"
				}
			}
		}
	}
	return ""
}

func (c *Config) applyDefaults() {
	if c.Tag == "" {
		c.Tag = DefaultTag
	}
	if c.Poll.Interval == 0 {
		c.Poll.Interval = Duration(10 * time.Second)
	}
	if c.SSH.User == "" {
		c.SSH.User = "root"
	}
	if c.SSH.Port == 0 {
		c.SSH.Port = 22
	}
	for name, tier := range c.Tiers {
		if len(tier.Labels) == 0 && tier.RequiredLabel != "" {
			tier.Labels = []string{tier.RequiredLabel}
		}
		if tier.ResetMode == "" {
			tier.ResetMode = ResetNone
		}
		if tier.ResetMinRemaining == 0 {
			tier.ResetMinRemaining = Duration(10 * time.Minute)
		}
		if tier.ResetTimeout == 0 {
			tier.ResetTimeout = Duration(5 * time.Minute)
		}
		if tier.IdleTimeout == 0 {
			tier.IdleTimeout = Duration(5 * time.Minute)
		}
		if tier.HourMargin == 0 {
			tier.HourMargin = Duration(5 * time.Minute)
		}
		if tier.BillingHour == 0 {
			tier.BillingHour = Duration(time.Hour)
		}
		if tier.MaxInstances == 0 {
			tier.MaxInstances = 1
		}
		c.Tiers[name] = tier
	}
	c.Routing.applyDefaults(c.Tiers)
	c.Transport.applyDefaults()
}

func (r *Routing) applyDefaults(tiers map[string]Tier) {
	r.Currency = strings.ToUpper(strings.TrimSpace(r.Currency))
	if len(r.Routes) == 0 {
		return
	}
	normalizedRates := make(map[string]string, len(r.ExchangeRates)+1)
	for currency, rate := range r.ExchangeRates {
		normalizedRates[strings.ToUpper(strings.TrimSpace(currency))] = strings.TrimSpace(rate)
	}
	if r.Currency != "" {
		if _, ok := normalizedRates[r.Currency]; !ok {
			normalizedRates[r.Currency] = "1"
		}
	}
	r.ExchangeRates = normalizedRates
	for name, route := range r.Routes {
		if route.HistoryWindow == 0 {
			route.HistoryWindow = Duration(720 * time.Hour)
		}
		if route.MinSamples == 0 {
			route.MinSamples = 10
		}
		if route.ColdStartP95 == 0 {
			route.ColdStartP95 = Duration(15 * time.Minute)
		}
		r.Routes[name] = route
		for _, candidate := range route.Candidates {
			tier, ok := tiers[candidate]
			if !ok || slices.Contains(forgejo.BareLabels(tier.Labels), route.RequiredLabel) {
				continue
			}
			tier.Labels = append(tier.Labels, route.RequiredLabel)
			tiers[candidate] = tier
		}
	}
}

func (c *Config) validate() error {
	if err := c.validateRequiredFields(); err != nil {
		return err
	}
	if c.Poll.Interval.D() <= 0 {
		return errors.New("config: poll.interval must be positive")
	}
	if err := c.validateDatabase(); err != nil {
		return err
	}
	needsSSH, err := c.validateProviders()
	if err != nil {
		return err
	}
	if needsSSH && c.SSH.PrivateKeyFile == "" {
		return errors.New("config: missing required fields: ssh.private_key_file")
	}
	if err := c.validateRouting(); err != nil {
		return err
	}
	if err := c.validateTiers(); err != nil {
		return err
	}
	if err := c.validateTransportProvider(); err != nil {
		return err
	}
	if err := c.Transport.validate(); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	return nil
}

func (c *Config) validateRequiredFields() error {
	var missing []string
	if c.Forgejo.URL == "" {
		missing = append(missing, "forgejo.url")
	}
	if c.Forgejo.Token == "" {
		missing = append(missing, "forgejo.token")
	}
	if c.Forgejo.Scope == "" {
		missing = append(missing, "forgejo.scope")
	}
	if c.Database.Path == "" {
		missing = append(missing, "database.path")
	}
	if len(c.Providers) == 0 {
		missing = append(missing, "providers")
	}
	if len(c.Tiers) == 0 {
		missing = append(missing, "tiers")
	}
	if len(missing) > 0 {
		return fmt.Errorf("config: missing required fields: %s", strings.Join(missing, ", "))
	}
	return nil
}

func (c *Config) validateDatabase() error {
	if c.Database.Retention.D() < 0 {
		return errors.New("config: database.retention must not be negative")
	}
	if dir := filepath.Dir(c.Database.Path); dir == "" {
		return errors.New("config: database.path has no parent directory")
	} else if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return fmt.Errorf("config: database.path parent %q must exist and be a directory", dir)
	}
	return nil
}

func (c *Config) validateProviders() (bool, error) {
	needsSSH := false
	for name, p := range c.Providers {
		if p.Driver == "" {
			return false, fmt.Errorf("config: providers.%s.driver is required", name)
		}
		needsSSH = needsSSH || p.Driver != ProviderDocker
	}
	return needsSSH, nil
}

func (c *Config) validateRouting() error {
	if len(c.Routing.Routes) == 0 {
		return nil
	}
	if len(c.Routing.Currency) != 3 {
		return errors.New("config: routing.currency must be a three-letter currency")
	}
	for _, char := range c.Routing.Currency {
		if char < 'A' || char > 'Z' {
			return errors.New("config: routing.currency must be a three-letter currency")
		}
	}
	for currency, rate := range c.Routing.ExchangeRates {
		if len(currency) != 3 {
			return fmt.Errorf("config: routing.exchange_rates.%s is not a three-letter currency", currency)
		}
		value, ok := new(big.Rat).SetString(rate)
		if !ok || value.Sign() <= 0 {
			return fmt.Errorf("config: routing.exchange_rates.%s must be a positive decimal", currency)
		}
	}
	baseRate, ok := new(big.Rat).SetString(c.Routing.ExchangeRates[c.Routing.Currency])
	if !ok || baseRate.Cmp(big.NewRat(1, 1)) != 0 {
		return fmt.Errorf("config: routing.exchange_rates.%s must equal 1", c.Routing.Currency)
	}
	return c.validateRoutes()
}

func (c *Config) validateRoutes() error {
	labels := make(map[string]string, len(c.Routing.Routes))
	for name, route := range c.Routing.Routes {
		prefix := "config: routing.routes." + name
		if strings.TrimSpace(name) == "" || route.RequiredLabel == "" {
			return fmt.Errorf("%s.required_label is required", prefix)
		}
		if owner := labels[route.RequiredLabel]; owner != "" {
			return fmt.Errorf("%s.required_label duplicates route %s", prefix, owner)
		}
		labels[route.RequiredLabel] = name
		if len(route.Candidates) < 2 {
			return fmt.Errorf("%s.candidates must contain at least two tiers", prefix)
		}
		seen := make(map[string]struct{}, len(route.Candidates))
		for _, candidate := range route.Candidates {
			if _, ok := c.Tiers[candidate]; !ok {
				return fmt.Errorf("%s.candidates references unknown tier %q", prefix, candidate)
			}
			if _, duplicate := seen[candidate]; duplicate {
				return fmt.Errorf("%s.candidates contains duplicate tier %q", prefix, candidate)
			}
			seen[candidate] = struct{}{}
		}
		if _, ok := seen[route.FallbackTier]; !ok {
			return fmt.Errorf("%s.fallback_tier must be one of candidates", prefix)
		}
		if route.HistoryWindow.D() <= 0 || route.MinSamples <= 0 || route.ColdStartP95.D() <= 0 {
			return fmt.Errorf("%s history_window, min_samples, and cold_start_p95 must be positive", prefix)
		}
		if route.MaxOptimizationWaitQueue < 0 {
			return fmt.Errorf("%s.max_optimization_wait_queue must not be negative", prefix)
		}
	}
	return c.validateRouteLabels(labels)
}

func (c *Config) validateRouteLabels(routes map[string]string) error {
	for tierName, tier := range c.Tiers {
		if route := routes[tier.RequiredLabel]; route != "" {
			return fmt.Errorf("config: tier %s required_label is owned by routing route %s", tierName, route)
		}
		candidateFor := make(map[string]struct{})
		for routeName, route := range c.Routing.Routes {
			if slices.Contains(route.Candidates, tierName) {
				candidateFor[routeName] = struct{}{}
			}
		}
		for _, label := range forgejo.BareLabels(tier.Labels) {
			routeName := routes[label]
			if routeName == "" {
				continue
			}
			if _, ok := candidateFor[routeName]; !ok {
				return fmt.Errorf("config: tier %s advertises route label %q but is not a candidate", tierName, label)
			}
		}
	}
	return nil
}

func (c *Config) validateTiers() error {
	requiredOwners := make(map[string]string, len(c.Tiers))
	for name, tier := range c.Tiers {
		if err := c.validateTier(name, tier); err != nil {
			return err
		}
		if prior := requiredOwners[tier.RequiredLabel]; prior != "" {
			return fmt.Errorf("config: tiers.%s.required_label duplicates tiers.%s", name, prior)
		}
		requiredOwners[tier.RequiredLabel] = name
	}
	for name, tier := range c.Tiers {
		for _, label := range tier.Labels {
			if owner := requiredOwners[label]; owner != "" && owner != name {
				return fmt.Errorf("config: tiers.%s.labels advertises required label owned by tier %s", name, owner)
			}
		}
	}
	return nil
}

func (c *Config) validateTransportProvider() error {
	if c.Transport.Mode == TransportCacheGateway {
		if len(c.Tiers) != 1 {
			return errors.New("config: transport.mode=cache-gateway requires exactly one tier")
		}
		for _, tier := range c.Tiers {
			if c.Providers[tier.Provider].Driver != "linode" {
				return errors.New("config: transport.mode=cache-gateway requires a linode tier")
			}
		}
	}
	return nil
}

func (c *Config) validateTier(name string, tier Tier) error {
	prefix := "config: tiers." + name
	p, err := c.validateTierRouting(prefix, tier)
	if err != nil {
		return err
	}
	if tier.InstanceType == "" && p.Driver != ProviderDocker {
		return fmt.Errorf("%s.instance_type is required", prefix)
	}
	if err := validateTierCapacity(prefix, tier); err != nil {
		return err
	}
	if err := validateTierTimings(prefix, tier); err != nil {
		return err
	}
	return validateTierReset(prefix, tier)
}

func (c *Config) validateTierRouting(prefix string, tier Tier) (ProviderInstance, error) {
	if tier.RequiredLabel == "" {
		return ProviderInstance{}, fmt.Errorf("%s.required_label is required", prefix)
	}
	if !slices.Contains(forgejo.BareLabels(tier.Labels), tier.RequiredLabel) {
		return ProviderInstance{}, fmt.Errorf("%s.labels must advertise required_label %q", prefix, tier.RequiredLabel)
	}
	p, ok := c.Providers[tier.Provider]
	if tier.Provider == "" || !ok {
		return ProviderInstance{}, fmt.Errorf("%s.provider %q is not defined", prefix, tier.Provider)
	}
	return p, nil
}

func validateTierCapacity(prefix string, tier Tier) error {
	if tier.MaxInstances < 1 {
		return fmt.Errorf("%s.max_instances must be positive", prefix)
	}
	if tier.WarmInstances < 0 || tier.WarmInstances > tier.MaxInstances {
		return fmt.Errorf("%s.warm_instances must be between zero and max_instances", prefix)
	}
	return nil
}

func validateTierTimings(prefix string, tier Tier) error {
	for _, timing := range []struct {
		field string
		value time.Duration
	}{
		{field: "reset_min_remaining", value: tier.ResetMinRemaining.D()},
		{field: "reset_timeout", value: tier.ResetTimeout.D()},
		{field: "idle_timeout", value: tier.IdleTimeout.D()},
		{field: "billing_hour", value: tier.BillingHour.D()},
	} {
		if timing.value <= 0 {
			return fmt.Errorf("%s.%s must be positive", prefix, timing.field)
		}
	}
	if margin := tier.HourMargin.D(); margin < 0 || margin >= tier.BillingHour.D() {
		return fmt.Errorf("%s.hour_margin must be non-negative and less than billing_hour", prefix)
	}
	return nil
}

func validateTierReset(prefix string, tier Tier) error {
	if tier.ResetMode != ResetNone && tier.ResetMode != ResetSnapshot {
		return fmt.Errorf("%s.reset_mode must be %q or %q", prefix, ResetNone, ResetSnapshot)
	}
	if tier.ResetMode == ResetSnapshot && !tier.OneJobPerVM {
		return fmt.Errorf("%s.reset_mode=snapshot requires one_job_per_vm=true", prefix)
	}
	return nil
}

// TierNames returns stable lexical ordering for maps used by wiring and APIs.
func (c *Config) TierNames() []string {
	out := make([]string, 0, len(c.Tiers))
	for name := range c.Tiers {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

const (
	tierTagDomain    = "fj-bellows-tier-tag/v1"
	tierTagPrefix    = "fjb-t-"
	tierTagHashBytes = 20
)

// TierTag scopes provider ownership to one deployment and tier. The
// length-prefixed tuple avoids separator ambiguity, while the conservative
// 46-character output fits every supported provider's tag/label projection.
func (c *Config) TierTag(tier string) string {
	h := sha256.New()
	_, _ = h.Write([]byte(tierTagDomain))
	var size [8]byte
	for _, component := range []string{c.Tag, tier} {
		binary.BigEndian.PutUint64(size[:], uint64(len(component)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(component))
	}
	sum := h.Sum(nil)
	return tierTagPrefix + hex.EncodeToString(sum[:tierTagHashBytes])
}

// Duration is a time.Duration decoded from a Go duration string.
type Duration time.Duration

// UnmarshalYAML parses a Go duration string from a scalar YAML node.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	pd, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(pd)
	return nil
}

// D returns the wrapped duration.
func (d Duration) D() time.Duration { return time.Duration(d) }
