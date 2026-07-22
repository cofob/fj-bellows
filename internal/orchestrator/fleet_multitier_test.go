package orchestrator

import (
	"context"
	"strings"
	"testing"
	"time"

	omock "github.com/hstern/fj-bellows/internal/orchestrator/mock"
	"github.com/hstern/fj-bellows/internal/provider"
	pmock "github.com/hstern/fj-bellows/internal/provider/mock"
)

const (
	fleetLongTier      = "long"
	fleetShortTier     = "short"
	fleetShortIP       = "192.0.2.10"
	fleetHetznerName   = "hetzner-primary"
	fleetHetznerDriver = "hetzner"
)

func fleetTestOrchestrator(tier, providerName, driver string, prov provider.Provider, disp Dispatcher) *Orchestrator {
	cfg := baseConfig()
	cfg.Tier = tier
	cfg.ProviderName = providerName
	cfg.Driver = driver
	return New(cfg, prov, &omock.JobSource{}, disp, nil)
}

func TestFleetPoolSnapshotIncludesTierAndProviderMetadata(t *testing.T) {
	short := fleetTestOrchestrator(
		fleetShortTier, "do-primary", "digitalocean", &pmock.Provider{}, &omock.Dispatcher{},
	)
	long := fleetTestOrchestrator(
		fleetLongTier, fleetHetznerName, fleetHetznerDriver, &pmock.Provider{}, &omock.Dispatcher{},
	)
	short.pool.Put(&Node{InstanceID: "short-b", State: StateIdle})
	short.pool.Put(&Node{InstanceID: "short-a", State: StateBusy})
	long.pool.Put(&Node{InstanceID: "long-a", State: StateProvisioning})

	fleet, err := NewFleet(map[string]*Orchestrator{fleetShortTier: short, fleetLongTier: long})
	if err != nil {
		t.Fatalf("NewFleet: %v", err)
	}
	got := fleet.PoolSnapshot()
	if len(got) != 3 {
		t.Fatalf("PoolSnapshot length = %d, want 3", len(got))
	}

	want := []struct {
		tier, providerName, driver, id string
	}{
		{fleetLongTier, fleetHetznerName, fleetHetznerDriver, "long-a"},
		{fleetShortTier, "do-primary", "digitalocean", "short-a"},
		{fleetShortTier, "do-primary", "digitalocean", "short-b"},
	}
	for i := range want {
		if got[i].Tier != want[i].tier || got[i].ProviderName != want[i].providerName ||
			got[i].Driver != want[i].driver || got[i].InstanceID != want[i].id {
			t.Errorf("worker[%d] = {tier:%q provider:%q driver:%q id:%q}, want %+v",
				i, got[i].Tier, got[i].ProviderName, got[i].Driver, got[i].InstanceID, want[i])
		}
	}
}

func TestFleetTierSelectorsRejectMissingUnknownAndAmbiguousTargets(t *testing.T) {
	short := fleetTestOrchestrator(
		fleetShortTier, "do-primary", "digitalocean", &pmock.Provider{}, &SSHDispatcher{},
	)
	long := fleetTestOrchestrator(
		fleetLongTier, fleetHetznerName, fleetHetznerDriver, &pmock.Provider{}, &SSHDispatcher{},
	)
	short.pool.Put(&Node{InstanceID: "shared", State: StateIdle})
	long.pool.Put(&Node{InstanceID: "shared", State: StateIdle})
	short.pool.Put(&Node{InstanceID: "short-only", State: StateIdle})

	fleet, err := NewFleet(map[string]*Orchestrator{fleetShortTier: short, fleetLongTier: long})
	if err != nil {
		t.Fatalf("NewFleet: %v", err)
	}

	assertContains := func(name string, err error, want string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s error = %v, want substring %q", name, err, want)
		}
	}

	_, err = fleet.ForceProvisionIn(t.Context(), "")
	assertContains("ForceProvision without tier", err, "tier selector is required")
	_, err = fleet.ForceProvisionIn(t.Context(), "missing")
	assertContains("ForceProvision unknown tier", err, `unknown tier "missing"`)

	err = fleet.ForceReapIn(t.Context(), "", "shared")
	assertContains("ForceReap ambiguous instance", err, "matched 2 tiers")
	err = fleet.ForceReapIn(t.Context(), "", "absent")
	assertContains("ForceReap absent instance", err, "matched 0 tiers")
	err = fleet.ForceReapIn(t.Context(), "missing", "shared")
	assertContains("ForceReap unknown tier", err, `unknown tier "missing"`)

	_, err = fleet.ExecOnWorkerIn(t.Context(), "", "shared", "true")
	assertContains("Exec ambiguous instance", err, "is ambiguous")
	_, err = fleet.ExecOnWorkerIn(t.Context(), "", "absent", "true")
	assertContains("Exec absent instance", err, "not in fleet")
	_, err = fleet.ExecOnWorkerIn(t.Context(), "missing", "shared", "true")
	assertContains("Exec unknown tier", err, `unknown tier "missing"`)
	_, err = fleet.ExecOnWorkerIn(t.Context(), fleetLongTier, "short-only", "true")
	assertContains("Exec scoped to wrong tier", err, `instance "short-only" not in pool`)
}

func TestFleetForceActionsRouteToSelectedTier(t *testing.T) {
	shortProvider := &pmock.Provider{
		ProvisionFn: func(context.Context, provider.Spec) (provider.Instance, error) {
			return provider.Instance{ID: "short-vm", IPv4: fleetShortIP, CreatedAt: time.Now()}, nil
		},
	}
	longProvider := &pmock.Provider{
		ProvisionFn: func(context.Context, provider.Spec) (provider.Instance, error) {
			return provider.Instance{ID: "long-vm", IPv4: "192.0.2.20", CreatedAt: time.Now()}, nil
		},
	}
	short := fleetTestOrchestrator(
		fleetShortTier, "do-primary", "digitalocean", shortProvider, &omock.Dispatcher{},
	)
	long := fleetTestOrchestrator(
		fleetLongTier, fleetHetznerName, fleetHetznerDriver, longProvider, &omock.Dispatcher{},
	)
	runOrchestrator(t, short)
	runOrchestrator(t, long)

	fleet, err := NewFleet(map[string]*Orchestrator{fleetShortTier: short, fleetLongTier: long})
	if err != nil {
		t.Fatalf("NewFleet: %v", err)
	}
	id, err := fleet.ForceProvisionIn(t.Context(), fleetLongTier)
	if err != nil {
		t.Fatalf("ForceProvisionIn(long): %v", err)
	}
	if id != "long-vm" {
		t.Fatalf("ForceProvisionIn(long) id = %q, want long-vm", id)
	}
	if shortProvider.ProvisionCount() != 0 || longProvider.ProvisionCount() != 1 {
		t.Fatalf("provision counts = short:%d long:%d, want short:0 long:1",
			shortProvider.ProvisionCount(), longProvider.ProvisionCount())
	}

	if err := fleet.ForceReapIn(t.Context(), fleetLongTier, id); err != nil {
		t.Fatalf("ForceReapIn(long): %v", err)
	}
	if shortProvider.DestroyCount() != 0 || longProvider.DestroyCount() != 1 {
		t.Fatalf("destroy counts = short:%d long:%d, want short:0 long:1",
			shortProvider.DestroyCount(), longProvider.DestroyCount())
	}
}
