package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func loadExample(t *testing.T) *Config {
	t.Helper()
	raw, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	databasePath := filepath.Join(t.TempDir(), "fj-bellows.db")
	yaml := strings.Replace(string(raw), "/var/lib/fj-bellows/fj-bellows.db", databasePath, 1)
	path := writeTemp(t, "config.yaml", yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load example: %v", err)
	}
	return cfg
}

func TestExampleParsesAsMultiProviderFleet(t *testing.T) {
	cfg := loadExample(t)
	if cfg.Transport.Mode != TransportSSH {
		t.Fatalf("Transport.Mode = %q, want %q", cfg.Transport.Mode, TransportSSH)
	}
	if got := cfg.TierNames(); !reflect.DeepEqual(got, []string{"long", "short"}) {
		t.Fatalf("TierNames = %v", got)
	}
	if cfg.Providers["digitalocean"].Driver != "digitalocean" {
		t.Fatalf("DigitalOcean provider = %#v", cfg.Providers["digitalocean"])
	}
	if cfg.Providers["hetzner"].Driver != "hetzner" {
		t.Fatalf("Hetzner provider = %#v", cfg.Providers["hetzner"])
	}
	short := cfg.Tiers["short"]
	if short.WarmInstances != 0 || short.ResetMode != ResetNone || !short.OneJobPerVM {
		t.Fatalf("short cold tier = %#v", short)
	}
	long := cfg.Tiers["long"]
	if long.WarmInstances != 0 || long.ResetMode != ResetSnapshot || !long.OneJobPerVM {
		t.Fatalf("long snapshot tier = %#v", long)
	}
}
