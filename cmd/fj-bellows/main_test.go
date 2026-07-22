package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/hstern/fj-bellows/internal/bootstrap"
	"github.com/hstern/fj-bellows/internal/config"
	"github.com/hstern/fj-bellows/internal/forgejo"
	"github.com/hstern/fj-bellows/internal/orchestrator"
	"github.com/hstern/fj-bellows/internal/provider"
)

const (
	testHetznerDriver = "hetzner"
	testHetznerType   = "cx33"
)

type compositionProvider struct {
	billing provider.BillingModel
}

func (p *compositionProvider) Configure(context.Context, string, yaml.Node) error { return nil }

func (p *compositionProvider) Provision(context.Context, provider.Spec) (provider.Instance, error) {
	return provider.Instance{}, nil
}

func (p *compositionProvider) Destroy(context.Context, string) error { return nil }

func (p *compositionProvider) List(context.Context, string) ([]provider.Instance, error) {
	return nil, nil
}

func (p *compositionProvider) BillingModel() provider.BillingModel { return p.billing }

type resetCapability struct{}

func (*resetCapability) Reset(context.Context, string, provider.ResetSpec) (provider.Instance, error) {
	return provider.Instance{}, nil
}

type imageCapability struct{}

func (*imageCapability) CreateImage(context.Context, provider.ImageSpec) (provider.ManagedImage, error) {
	return provider.ManagedImage{}, nil
}

func (*imageCapability) DeleteImage(context.Context, string) error { return nil }

func (*imageCapability) ListImages(context.Context, string) ([]provider.ManagedImage, error) {
	return nil, nil
}

type builderCapability struct{}

func (*builderCapability) ListBuilders(context.Context, string) ([]provider.Instance, error) {
	return nil, nil
}

type snapshotCapabilityProvider struct {
	compositionProvider
	resetCapability
	imageCapability
	builderCapability
}

func (*snapshotCapabilityProvider) PromoteBuilder(context.Context, string, string) error { return nil }

type providerWithoutReset struct {
	compositionProvider
	imageCapability
	builderCapability
}

type providerWithoutImages struct {
	compositionProvider
	resetCapability
	builderCapability
}

type providerWithoutBuilders struct {
	compositionProvider
	resetCapability
	imageCapability
}

type providerWithoutPromotion struct {
	compositionProvider
	resetCapability
	imageCapability
	builderCapability
}

type compositionDispatcher struct{}

func (*compositionDispatcher) WaitReady(context.Context, string, string) error { return nil }

func (*compositionDispatcher) RunJob(
	context.Context,
	string,
	string,
	forgejo.Registration,
	forgejo.WaitingJob,
) error {
	return nil
}

type imagePreparationCapability struct{}

func (*imagePreparationCapability) PrepareImage(context.Context, string, string) error { return nil }

type snapshotCapabilityDispatcher struct {
	compositionDispatcher
	imagePreparationCapability
}

func TestValidateTierCapabilitiesSnapshotMatrix(t *testing.T) {
	fullyCapable := &snapshotCapabilityProvider{}
	imageDispatcher := &snapshotCapabilityDispatcher{}
	tier := config.Tier{ResetMode: config.ResetSnapshot}
	if err := validateTierCapabilities("long", tier, fullyCapable, imageDispatcher); err != nil {
		t.Fatalf("fully capable snapshot tier rejected: %v", err)
	}

	tests := []struct {
		name       string
		provider   provider.Provider
		dispatcher orchestrator.Dispatcher
		wantError  string
	}{
		{
			name: "resetter", provider: &providerWithoutReset{}, dispatcher: imageDispatcher,
			wantError: "in-place snapshot reset",
		},
		{
			name: "managed images", provider: &providerWithoutImages{}, dispatcher: imageDispatcher,
			wantError: "managed images",
		},
		{
			name: "builder recovery", provider: &providerWithoutBuilders{}, dispatcher: imageDispatcher,
			wantError: "image builders for crash recovery",
		},
		{
			name: "builder hand-off", provider: &providerWithoutPromotion{}, dispatcher: imageDispatcher,
			wantError: "cost-effective image-builder hand-off",
		},
		{
			name: "image preparation", provider: fullyCapable, dispatcher: &compositionDispatcher{},
			wantError: "golden-image preparation",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateTierCapabilities("long", tier, test.provider, test.dispatcher)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("validateTierCapabilities() error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

func TestBuildOrchestratorConfigMapsTierAndProvider(t *testing.T) {
	cfg := &config.Config{
		Forgejo: config.Forgejo{URL: "https://forgejo.example.test/", Scope: "org"},
		Providers: map[string]config.ProviderInstance{
			"hetzner-primary": {Driver: testHetznerDriver},
		},
		Poll:      config.Poll{Interval: config.Duration(7 * time.Second)},
		Transport: config.Transport{Mode: "cache-gateway"},
		Tag:       "ci-fleet",
	}
	tier := config.Tier{
		RequiredLabel:     "ci-long-amd64",
		Labels:            []string{"ci-long-amd64", "linux", "amd64"},
		Provider:          "hetzner-primary",
		InstanceType:      testHetznerType,
		OneJobPerVM:       true,
		ResetMode:         config.ResetSnapshot,
		ResetMinRemaining: config.Duration(11 * time.Minute),
		ResetTimeout:      config.Duration(6 * time.Minute),
		WarmInstances:     3,
		IdleTimeout:       config.Duration(2 * time.Minute),
		HourMargin:        config.Duration(4 * time.Minute),
		BillingHour:       config.Duration(time.Hour),
		MaxInstances:      10,
	}
	opts := runOpts{
		runnerVersion: "12.10.1-test", drain: true,
		drainTimeout: 23 * time.Minute, destroyOnExit: true,
	}
	prov := &compositionProvider{billing: provider.BillingHourlyRoundUp}

	got, err := buildOrchestratorConfig(cfg, "long", tier, opts, "v-test", "ssh-ed25519 test", prov)
	if err != nil {
		t.Fatal(err)
	}
	assertOrchestratorIdentity(t, got, cfg, tier)
	assertOrchestratorRuntime(t, got, cfg, tier, opts)
	if got.Teardown.Model != provider.BillingHourlyRoundUp {
		t.Fatalf("teardown billing model = %v, want provider model %v",
			got.Teardown.Model, provider.BillingHourlyRoundUp)
	}
}

func assertOrchestratorIdentity(
	t *testing.T,
	got orchestrator.Config,
	cfg *config.Config,
	tier config.Tier,
) {
	t.Helper()
	if got.Tier != "long" || got.ProviderName != tier.Provider || got.Driver != testHetznerDriver {
		t.Errorf("routing identity = tier %q, provider %q, driver %q",
			got.Tier, got.ProviderName, got.Driver)
	}
	if got.Tag != cfg.TierTag("long") || got.InstanceType != tier.InstanceType {
		t.Errorf("machine identity = tag %q, instance %q", got.Tag, got.InstanceType)
	}
	if strings.Join(got.Labels, ",") != strings.Join(tier.Labels, ",") {
		t.Errorf("labels = %v, want %v", got.Labels, tier.Labels)
	}
	if got.ForgejoSource != forgejoSource(cfg.Forgejo) || got.BootstrapFingerprint == "" {
		t.Errorf("durable identity = source %q, fingerprint %q", got.ForgejoSource, got.BootstrapFingerprint)
	}
}

func assertOrchestratorRuntime(
	t *testing.T,
	got orchestrator.Config,
	cfg *config.Config,
	tier config.Tier,
	opts runOpts,
) {
	t.Helper()
	assertCapacityAndTimings(t, got, tier)
	assertBootstrapRuntime(t, got, cfg, opts)
	assertShutdownRuntime(t, got, opts)
}

func assertCapacityAndTimings(t *testing.T, got orchestrator.Config, tier config.Tier) {
	t.Helper()
	if got.MaxScale != tier.MaxInstances || got.WarmInstances != tier.WarmInstances ||
		got.OneJobPerVM != tier.OneJobPerVM || got.ResetMode != tier.ResetMode {
		t.Errorf("capacity/reset mapping = max %d, warm %d, one-job %v, reset %q",
			got.MaxScale, got.WarmInstances, got.OneJobPerVM, got.ResetMode)
	}
	if got.ResetMinRemaining != tier.ResetMinRemaining.D() || got.ResetTimeout != tier.ResetTimeout.D() {
		t.Errorf("reset timings = minimum %v, timeout %v", got.ResetMinRemaining, got.ResetTimeout)
	}
	if got.Teardown.IdleTimeout != tier.IdleTimeout.D() ||
		got.Teardown.HourMargin != tier.HourMargin.D() || got.Teardown.BillingHour != tier.BillingHour.D() {
		t.Errorf("teardown timings = %+v", got.Teardown)
	}
}

func assertBootstrapRuntime(t *testing.T, got orchestrator.Config, cfg *config.Config, opts runOpts) {
	t.Helper()
	if got.PollInterval != cfg.Poll.Interval.D() || got.RunnerVersion != opts.runnerVersion ||
		got.ReadyFile != bootstrap.DefaultReadyFile || got.TransportMode != cfg.Transport.Mode {
		t.Errorf("runtime mapping = poll %v, runner %q, ready %q, transport %q",
			got.PollInterval, got.RunnerVersion, got.ReadyFile, got.TransportMode)
	}
	if got.AuthorizedKey != "ssh-ed25519 test" || got.FJBAgentDownloadURL != "" || got.FJBAgentToken != "" {
		t.Errorf("bootstrap credentials = key %q, agent URL %q, agent token %q",
			got.AuthorizedKey, got.FJBAgentDownloadURL, got.FJBAgentToken)
	}
}

func assertShutdownRuntime(t *testing.T, got orchestrator.Config, opts runOpts) {
	t.Helper()
	if got.DrainOnShutdown != opts.drain || got.DrainTimeout != opts.drainTimeout ||
		got.DestroyOnExit != opts.destroyOnExit {
		t.Errorf("shutdown mapping = drain %v, timeout %v, destroy %v",
			got.DrainOnShutdown, got.DrainTimeout, got.DestroyOnExit)
	}
}
