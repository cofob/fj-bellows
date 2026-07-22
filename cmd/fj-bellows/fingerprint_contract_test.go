package main

import (
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/hstern/fj-bellows/internal/bootstrap"
	"github.com/hstern/fj-bellows/internal/config"
)

func TestBootstrapFingerprintIncludesSnapshotContractMarker(t *testing.T) {
	var providerConfig yaml.Node
	if err := yaml.Unmarshal([]byte("token: test\nlocation: test\nimage: test\n"), &providerConfig); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Providers: map[string]config.ProviderInstance{
		"primary": {Driver: testHetznerDriver, Config: providerConfig},
	}}
	tier := config.Tier{Provider: "primary", InstanceType: testHetznerType}

	actual := bootstrapFingerprint(cfg, "long", tier, "1.2.3", "https://agent.example/binary")
	want := bootstrapFingerprintWithContract(
		cfg, "long", tier, "1.2.3", "https://agent.example/binary", bootstrap.SnapshotContractMarker(),
	)
	if actual != want {
		t.Fatalf("bootstrapFingerprint = %q, want contract-aware %q", actual, want)
	}
	before := bootstrapFingerprintWithContract(cfg, "long", tier, "1.2.3", "https://agent.example/binary", "contract-v1")
	after := bootstrapFingerprintWithContract(cfg, "long", tier, "1.2.3", "https://agent.example/binary", "contract-v2")
	if before == after {
		t.Fatalf("snapshot contract change did not invalidate fingerprint %q", before)
	}
}
